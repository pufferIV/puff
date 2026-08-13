// Fonction serverless Netlify : proxy vers Edge-TTS (Microsoft)
// Cette fonction tourne côté serveur (Node.js), elle peut donc définir
// le header "Origin" que les navigateurs interdisent de modifier en JS.
// C'est ce qui permet à Edge-TTS de fonctionner alors qu'un appel direct
// depuis le navigateur/WebView échoue.

const WebSocket = require('ws');
const crypto = require('crypto');

const TRUSTED_TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4";
const CHROMIUM_VERSION = "130.0.2849.68";
const CHROMIUM_MAJOR = "130";

async function generateSecMsGec() {
    const WIN_EPOCH = 11644473600; // secondes entre 1601-01-01 et 1970-01-01
    let ticks = Math.floor(Date.now() / 1000) + WIN_EPOCH;
    ticks = ticks - (ticks % 300); // arrondi au créneau de 5 minutes
    ticks = ticks * 10000000; // conversion en ticks Windows (100-ns)
    const strToHash = ticks.toString() + TRUSTED_TOKEN;
    return crypto.createHash('sha256').update(strToHash).digest('hex').toUpperCase();
}

function randomHexId(len) {
    let id = '';
    for (let i = 0; i < len; i++) id += Math.floor(Math.random() * 16).toString(16);
    return id;
}

function escapeXml(str) {
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function synthesize(text, voiceName, ratePercent, pitchHz) {
    return new Promise(async (resolve, reject) => {
        let secMsGec;
        try {
            secMsGec = await generateSecMsGec();
        } catch (e) {
            reject(e);
            return;
        }

        const connectionId = randomHexId(32);
        const wsUrl = `wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1?TrustedClientToken=${TRUSTED_TOKEN}&Sec-MS-GEC=${secMsGec}&Sec-MS-GEC-Version=1-${CHROMIUM_VERSION}&ConnectionId=${connectionId}`;

        const ws = new WebSocket(wsUrl, {
            headers: {
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Origin': 'chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold',
                'User-Agent': `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROMIUM_MAJOR}.0.0.0 Safari/537.36 Edg/${CHROMIUM_MAJOR}.0.0.0`,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9'
            }
        });

        let chunks = [];

        const timeout = setTimeout(() => {
            try { ws.terminate(); } catch (e) {}
            reject(new Error('Timeout Edge-TTS (15s)'));
        }, 15000);

        ws.on('open', () => {
            const timestamp = new Date().toString();

            const configMsg =
                `X-Timestamp:${timestamp}\r\n` +
                `Content-Type:application/json; charset=utf-8\r\n` +
                `Path:speech.config\r\n\r\n` +
                JSON.stringify({
                    context: {
                        synthesis: {
                            audio: {
                                metadataoptions: {
                                    sentenceBoundaryEnabled: "false",
                                    wordBoundaryEnabled: "false"
                                },
                                outputFormat: "audio-24khz-48kbitrate-mono-mp3"
                            }
                        }
                    }
                });
            ws.send(configMsg);

            const rateStr = ratePercent >= 0 ? `+${ratePercent}%` : `${ratePercent}%`;
            const pitchStr = pitchHz >= 0 ? `+${pitchHz}Hz` : `${pitchHz}Hz`;

            const ssml =
                `<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='ru-RU'>` +
                `<voice name='${voiceName}'>` +
                `<prosody pitch='${pitchStr}' rate='${rateStr}' volume='+0%'>` +
                `${escapeXml(text)}` +
                `</prosody></voice></speak>`;

            const requestId = randomHexId(32);
            const ssmlMsg =
                `X-RequestId:${requestId}\r\n` +
                `Content-Type:application/ssml+xml\r\n` +
                `X-Timestamp:${timestamp}Z\r\n` +
                `Path:ssml\r\n\r\n` +
                ssml;

            ws.send(ssmlMsg);
        });

        ws.on('message', (data, isBinary) => {
            if (!isBinary) {
                const str = data.toString();
                if (str.includes('Path:turn.end')) {
                    clearTimeout(timeout);
                    ws.close();
                    resolve(Buffer.concat(chunks));
                }
            } else {
                // Message binaire : [2 octets = longueur en-tête][en-tête texte][données audio]
                const headerLength = data.readUInt16BE(0);
                const audioData = data.slice(2 + headerLength);
                chunks.push(audioData);
            }
        });

        ws.on('error', (err) => {
            clearTimeout(timeout);
            reject(err);
        });

        ws.on('close', () => {
            clearTimeout(timeout);
        });
    });
}

exports.handler = async (event) => {
    if (event.httpMethod !== 'POST') {
        return { statusCode: 405, body: 'Method not allowed' };
    }

    let body;
    try {
        body = JSON.parse(event.body);
    } catch (e) {
        return { statusCode: 400, body: 'JSON invalide' };
    }

    const { text, voice, rate, pitch } = body;
    if (!text || !voice) {
        return { statusCode: 400, body: 'Paramètres "text" et "voice" requis' };
    }
    if (text.length > 5000) {
        return { statusCode: 400, body: 'Texte trop long (max 5000 caractères)' };
    }

    try {
        const audioBuffer = await synthesize(text, voice, rate || 0, pitch || 0);
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'audio/mpeg',
                'Access-Control-Allow-Origin': '*'
            },
            body: audioBuffer.toString('base64'),
            isBase64Encoded: true
        };
    } catch (e) {
        console.error('Erreur Edge-TTS:', e);
        return {
            statusCode: 500,
            body: 'Erreur TTS: ' + (e && e.message ? e.message : String(e))
        };
    }
};
