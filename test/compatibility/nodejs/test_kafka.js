const net = require('net');
const { spawn, execSync } = require('child_process');
const path = require('path');
const fs = require('fs');

function findFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

function sendKafkaRequest(socket, apiKey, apiVersion, corrId, clientId, bodyBuf) {
  return new Promise((resolve, reject) => {
    const clientBuf = Buffer.from(clientId, 'utf-8');
    const header = Buffer.alloc(2 + 2 + 4 + 2 + clientBuf.length);
    header.writeInt16BE(apiKey, 0);
    header.writeInt16BE(apiVersion, 2);
    header.writeInt32BE(corrId, 4);
    header.writeInt16BE(clientBuf.length, 8);
    clientBuf.copy(header, 10);

    const payload = Buffer.concat([header, bodyBuf]);
    const frame = Buffer.alloc(4 + payload.length);
    frame.writeInt32BE(payload.length, 0);
    payload.copy(frame, 4);

    let expectedLen = null;
    let received = Buffer.alloc(0);

    function onData(chunk) {
      received = Buffer.concat([received, chunk]);
      if (expectedLen === null && received.length >= 4) {
        expectedLen = received.readInt32BE(0);
        received = received.slice(4);
      }
      if (expectedLen !== null && received.length >= expectedLen) {
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        resolve(received.slice(0, expectedLen));
      }
    }

    function onError(err) {
      socket.removeListener('data', onData);
      reject(err);
    }

    socket.on('data', onData);
    socket.once('error', onError);
    socket.write(frame);
  });
}

async function run() {
  const kafkaPort = await findFreePort();
  const redisPort = await findFreePort();

  const rootDir = path.resolve(__dirname, '..', '..', '..');
  const binName = process.platform === 'win32' ? 'pocketqueue_node_kafka.exe' : 'pocketqueue_node_kafka';
  const binPath = path.join(rootDir, binName);

  console.log(`[*] Building test binary at ${binPath}...`);
  execSync(`go build -o "${binPath}" ./cmd/pocketqueue`, { cwd: rootDir });

  console.log(`[*] Spawning PocketQueue (Kafka: ${kafkaPort}, Redis: ${redisPort})...`);
  const proc = spawn(binPath, [
    '--headless',
    '--host', '127.0.0.1',
    '--kafka-port', String(kafkaPort),
    '--redis-port', String(redisPort),
  ]);

  let socket;
  try {
    // Wait for Kafka port
    for (let i = 0; i < 30; i++) {
      try {
        socket = await new Promise((resolve, reject) => {
          const s = net.createConnection({ host: '127.0.0.1', port: kafkaPort }, () => resolve(s));
          s.once('error', reject);
        });
        break;
      } catch (e) {
        await new Promise((r) => setTimeout(r, 100));
      }
    }

    if (!socket) throw new Error('Failed to connect to PocketQueue Kafka server');

    console.log('[*] 1. Testing ApiVersions (API 18)...');
    const vResp = await sendKafkaRequest(socket, 18, 0, 201, 'node-client', Buffer.alloc(0));
    const corrId = vResp.readInt32BE(0);
    const errCode = vResp.readInt16BE(4);
    const numKeys = vResp.readInt32BE(6);
    if (corrId !== 201 || errCode !== 0 || numKeys <= 0) {
      throw new Error(`Unexpected ApiVersions response: corr=${corrId}, err=${errCode}, keys=${numKeys}`);
    }
    console.log(`    -> PASS (Keys advertised: ${numKeys})`);

    console.log('[*] 2. Testing CreateTopics (API 19)...');
    const topic = 'node-orders';
    const topicBuf = Buffer.from(topic, 'utf-8');
    const createBody = Buffer.alloc(4 + 2 + topicBuf.length + 4 + 2 + 4 + 4 + 4);
    let offset = 0;
    createBody.writeInt32BE(1, offset); offset += 4; // topics count
    createBody.writeInt16BE(topicBuf.length, offset); offset += 2;
    topicBuf.copy(createBody, offset); offset += topicBuf.length;
    createBody.writeInt32BE(2, offset); offset += 4; // 2 partitions
    createBody.writeInt16BE(1, offset); offset += 2; // repl factor 1
    createBody.writeInt32BE(0, offset); offset += 4; // assignments count 0
    createBody.writeInt32BE(0, offset); offset += 4; // configs count 0
    createBody.writeInt32BE(5000, offset); // timeout 5000ms

    const cResp = await sendKafkaRequest(socket, 19, 0, 202, 'node-client', createBody);
    const cCorr = cResp.readInt32BE(0);
    const cArr = cResp.readInt32BE(4);
    const cTLen = cResp.readInt16BE(8);
    const cTName = cResp.toString('utf-8', 10, 10 + cTLen);
    const cErr = cResp.readInt16BE(10 + cTLen);
    if (cCorr !== 202 || cArr !== 1 || cTName !== topic || cErr !== 0) {
      throw new Error(`CreateTopics failed: name=${cTName}, err=${cErr}`);
    }
    console.log(`    -> PASS (Created topic '${topic}' with 2 partitions)`);

    console.log('[*] 3. Testing Metadata (API 3)...');
    const metaBody = Buffer.alloc(4 + 2 + topicBuf.length);
    metaBody.writeInt32BE(1, 0);
    metaBody.writeInt16BE(topicBuf.length, 4);
    topicBuf.copy(metaBody, 6);
    const mResp = await sendKafkaRequest(socket, 3, 0, 203, 'node-client', metaBody);
    if (mResp.readInt32BE(0) !== 203) throw new Error('Metadata correlation mismatch');
    console.log(`    -> PASS`);

    console.log('[*] 4. Testing DeleteTopics (API 20)...');
    const delBody = Buffer.alloc(4 + 2 + topicBuf.length + 4);
    delBody.writeInt32BE(1, 0);
    delBody.writeInt16BE(topicBuf.length, 4);
    topicBuf.copy(delBody, 6);
    delBody.writeInt32BE(5000, 6 + topicBuf.length);
    const dResp = await sendKafkaRequest(socket, 20, 0, 204, 'node-client', delBody);
    const dCorr = dResp.readInt32BE(0);
    const dTLen = dResp.readInt16BE(8);
    const dTName = dResp.toString('utf-8', 10, 10 + dTLen);
    const dErr = dResp.readInt16BE(10 + dTLen);
    if (dCorr !== 204 || dTName !== topic || dErr !== 0) {
      throw new Error(`DeleteTopics failed: name=${dTName}, err=${dErr}`);
    }
    console.log(`    -> PASS (Deleted topic '${topic}')`);

    socket.end();
    console.log('\n[SUCCESS] All Node.js Kafka wire protocol compatibility tests passed!');
  } finally {
    if (socket) socket.destroy();
    proc.kill();
    if (fs.existsSync(binPath)) {
      try { fs.unlinkSync(binPath); } catch (e) {}
    }
  }
}

run().catch((err) => {
  console.error('[FAIL]', err);
  process.exit(1);
});
