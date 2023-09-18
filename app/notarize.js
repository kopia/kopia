require('dotenv').config();
const { notarize } = require('@electron/notarize');
const fs = require('fs');
const crypto = require('crypto');

exports.default = async function notarizing(context) {
  const { electronPlatformName, appOutDir } = context;  
  if (electronPlatformName !== 'darwin') {
    return;
  }

  if (!process.env.KOPIA_UI_NOTARIZE) {
    console.log('Not notarizing because KOPIA_UI_NOTARIZE is not set');
    return;
  }

  const appName = context.packager.appInfo.productFilename;

  const apiKey = Buffer.from(process.env.APPLE_API_KEY_BASE64, 'base64').toString('ascii');
  const keyFile = "/tmp/key-" +crypto.randomBytes(32).toString('hex');

  try {
    fs.writeFileSync(keyFile, apiKey);
    console.log('Submitting app for Apple notarization...')
    let timerId = setInterval(() => { console.log('Still waiting for notarization response...') }, 30000);
    let x =  await notarize({
      appBundleId: 'io.kopia.ui',
      appPath: `${appOutDir}/${appName}.app`,
      appleApiIssuer: process.env.APPLE_API_ISSUER,
      appleApiKeyId: process.env.APPLE_API_KEY_ID,
      appleApiKey: keyFile,
    });
    clearTimeout(timerId);
    return x;
  } finally {
    fs.unlinkSync(keyFile);
  }
};