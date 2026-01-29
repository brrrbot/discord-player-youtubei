import Innertube, { Constants, YT, YTNodes } from "youtubei.js";
import { getInnertube, getVideoId, passThroughStream, toNodeReadable } from "../utils";
import { getWebPoMinter, invalidateWebPoMinter } from "../Token/tokenGenerator";
import { SabrFormat } from "googlevideo/shared-types";
import { SabrStream, SabrStreamConfig } from "googlevideo/sabr-stream";
import { buildSabrFormat } from "googlevideo/utils";
import { DEFAULT_OPTIONS } from "../Constants";
import { Readable } from "node:stream";
import { CacheType, YoutubeTrack } from "../Classes";

export async function createSabrStream(video: YoutubeTrack, isLive: boolean = false): Promise<Readable | null> {
    console.log(`[SabrStream] Starting createSabrStream for video, isLive: ${isLive}`);

    const innertube: Innertube | null = await getInnertube();
    let accountInfo: YT.AccountInfo | null;
    const videoId: string = getVideoId(video.url);
    let serverAbrStream: SabrStream;

    console.log(`[SabrStream] Video ID: ${videoId}`);

    try {
        accountInfo = await innertube.account.getInfo();
        console.log(`[SabrStream] Account info retrieved successfully`);
    } catch (error) {
        console.log(`[SabrStream] No account info available`);
        accountInfo = null;
    }

    const dataSyncId = accountInfo?.contents?.contents[0]?.endpoint?.payload?.supportedTokens?.[2]?.datasyncIdToken?.datasyncIdToken ?? innertube.session.context.client.visitorData;
    console.log(`[SabrStream] DataSync ID obtained`);

    const minter = await getWebPoMinter(innertube);
    console.log(`[SabrStream] PoMinter initialized`);

    const contentPoToken = await minter.mint(videoId);
    console.log(`[SabrStream] Content PoToken minted`);

    const poToken = await minter.mint(dataSyncId);
    console.log(`[SabrStream] PoToken minted`);

    try {
        const videoData = video.getCache(CacheType.SeverAbr);
        let SabrStreamConfig: SabrStreamConfig;

        if (videoData) {
            console.log(`[SabrStream] Using cached video data`);
            SabrStreamConfig = {
                formats: videoData.sabrFormat,
                serverAbrStreamingUrl: videoData.url,
                videoPlaybackUstreamerConfig: videoData.uStreamConfig,
                poToken: poToken,
                clientInfo: {
                    clientName: parseInt(Constants.CLIENT_NAME_IDS[innertube.session.context.client.clientName]),
                    clientVersion: innertube.session.context.client.clientVersion,
                },
            }
        } else {
            console.log(`[SabrStream] No cache found, fetching fresh player response`);

            const watchEndpoint = new YTNodes.NavigationEndpoint({ watchEndpoint: { videoId } });
            console.log(`[SabrStream] Calling watchEndpoint...`);

            const playerResponse = await watchEndpoint.call(innertube.actions, {
                playbackContext: {
                    adPlaybackContext: { pyv: true },
                    contentPlaybackContext: {
                        vis: 0,
                        splay: false,
                        lactMilliseconds: "-1",
                        signatureTimestamp: innertube.session.player?.signature_timestamp,
                    },
                },
                contentCheckOk: true,
                racyCheckOk: true,
                serviceIntegrityDimensions: { poToken: contentPoToken },
                parse: true,
            });

            console.log(`[SabrStream] Player response received`);

            const serverAbrStreamingUrl = await innertube.session.player?.decipher(playerResponse.streaming_data?.server_abr_streaming_url);
            const videoPlaybackUstreamerConfig = playerResponse.player_config?.media_common_config.media_ustreamer_request_config?.video_playback_ustreamer_config;

            console.log(`[SabrStream] Server ABR URL available: ${!!serverAbrStreamingUrl}`);
            console.log(`[SabrStream] Ustreamer config available: ${!!videoPlaybackUstreamerConfig}`);

            if (!videoPlaybackUstreamerConfig) throw new Error("ustreamerConfig not found");
            if (!serverAbrStreamingUrl) throw new Error("serverAbrStreamingUrl not found");

            const allFormats: SabrFormat[] = playerResponse.streaming_data?.adaptive_formats.map(buildSabrFormat) || [];
            console.log(`[SabrStream] Total formats available: ${allFormats.length}`);

            // Log all available formats for debugging
            allFormats.forEach((fmt, idx) => {
                console.log(`[SabrStream] Format ${idx}: ${fmt.mimeType}, bitrate: ${fmt.bitrate}, hasAudio: ${fmt.mimeType?.includes('audio/') || (fmt.mimeType?.includes('codecs') && !fmt.mimeType?.includes('video/'))}`);
            });

            // Filter for MP4 audio-only formats
            const audioFormats = allFormats.filter(fmt => {
                const hasAudio = fmt.mimeType?.includes('audio/') ||
                    (fmt.mimeType?.includes('codecs') && !fmt.mimeType?.includes('video/'));

                if (!hasAudio) return false;

                return fmt.mimeType?.includes('mp4');
            });

            console.log(`[SabrStream] Filtered to ${audioFormats.length} MP4 audio formats`);

            // Sort by bitrate (higher quality first)
            audioFormats.sort((a, b) => {
                const aBitrate = a.bitrate || 0;
                const bBitrate = b.bitrate || 0;
                return bBitrate - aBitrate;
            });

            const sabrFormats = audioFormats.length > 0 ? audioFormats : allFormats;

            console.log(`[SabrStream] Selected ${sabrFormats.length} formats for ${isLive ? 'live' : 'regular'} video`);
            if (sabrFormats.length > 0) {
                console.log(`[SabrStream] Top format: ${sabrFormats[0].mimeType}, bitrate: ${sabrFormats[0].bitrate}`);
            }

            SabrStreamConfig = {
                formats: sabrFormats,
                serverAbrStreamingUrl,
                videoPlaybackUstreamerConfig,
                poToken: poToken,
                clientInfo: {
                    clientName: parseInt(Constants.CLIENT_NAME_IDS[innertube.session.context.client.clientName]),
                    clientVersion: innertube.session.context.client.clientVersion,
                },
            }
        }

        console.log(`[SabrStream] Creating SabrStream instance...`);
        serverAbrStream = new SabrStream(SabrStreamConfig);
        console.log(`[SabrStream] SabrStream instance created`);

    } catch (error) {
        console.error("[SabrStream Error] Error while creating SabrStream: ", error);
        throw error;
    }

    let protectionFailureCount = 0;
    let lastStatus = null;

    console.log(`[SabrStream] Setting up stream protection listener...`);
    serverAbrStream.on("streamProtectionStatusUpdate", async (statusUpdate: any) => {
        console.log(`[SabrStream] Stream protection status update: ${statusUpdate.status}`);
        if (statusUpdate.status !== lastStatus) lastStatus = statusUpdate.status;
        if (statusUpdate.status === 2) {
            protectionFailureCount = Math.min(protectionFailureCount + 1, 10);
            console.log(`[SabrStream] Protection failure count: ${protectionFailureCount}`);

            try {
                const rotationMinter = await getWebPoMinter(innertube, { forceRefresh: protectionFailureCount >= 3 });
                const placeholderToken = rotationMinter.generatePlaceholder(videoId);
                serverAbrStream.setPoToken(placeholderToken);
                const mintedPoToken = await rotationMinter.mint(videoId);
                serverAbrStream.setPoToken(mintedPoToken);
                console.log(`[SabrStream] PoToken rotated successfully`);
            } catch (error) {
                if (protectionFailureCount === 1 || protectionFailureCount % 5 === 0) console.error(`Failed to rotate PoToken: ${error}`);
            }
        } else if (statusUpdate.status === 3) {
            console.error("Stream protection rejected token (SPS 3). Resetting Botguard.");
            invalidateWebPoMinter();
        } else {
            protectionFailureCount = 0;
        }
    });

    // Add error listener
    // serverAbrStream.on("error", (error: any) => {
    //     console.error(`[SabrStream] Stream error event:`, error);
    // });

    // Add abort listener
    serverAbrStream.on("abort", () => {
        console.log(`[SabrStream] Stream aborted`);
    });

    // Add finish listener
    serverAbrStream.on("finish", () => {
        console.log(`[SabrStream] Stream finished`);
    });

    const playbackOptions = {
        ...DEFAULT_OPTIONS,
        preferMP4: true,
        preferWebM: false,
        preferH264: false,
        stallDetectionMs: isLive ? 20000 : 15000,
    };

    console.log(`[SabrStream] Playback options:`, playbackOptions);
    console.log(`[SabrStream] Starting stream...`);

    const { audioStream } = await serverAbrStream.start(playbackOptions);
    console.log(`[SabrStream] Stream started, creating Node stream...`);

    const nodeStream = passThroughStream(audioStream);
    console.log(`[SabrStream] Node stream created successfully`);

    return nodeStream;
}