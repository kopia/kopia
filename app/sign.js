const { spawnSync } = require("child_process");
exports.default = async function (configuration) {
  if (process.platform !== "win32") {
    return;
  }

  const sha1 = process.env.WINDOWS_CERT_SHA1;
  if (!sha1) {
    console.log('Not signing', configuration.path, ' because WINDOWS_CERT_SHA1 is not set');
    return;
  }

  const signTool = process.env.WINDOWS_SIGN_TOOL || "signtool.exe";

  console.log('signTool', signTool);
  
  const signToolArgs = [
    "sign",
    "/sha1", sha1,
    "/fd", configuration.hash,
    "/tr", "http://timestamp.digicert.com",
  ];

  if (configuration.isNest) {
    signToolArgs.push("/as")
  }

  signToolArgs.push("/v");
  signToolArgs.push(configuration.path);

  let nextSleepTime = 1000;

  for (let attempt = 0; attempt < 10; attempt++) {
    console.log('Signing ', configuration.path, 'attempt', attempt);
    if (attempt > 0) {
      console.log('Sleping for ', nextSleepTime);
      await new Promise(r => setTimeout(r, nextSleepTime));
    }
    nextSleepTime *= 2;

    const result = spawnSync(signTool, signToolArgs, {
      stdio: "inherit",
    });

    if (!result.error && 0 === result.status) {
      console.log('Signing of', configuration.path, ' succeeded');
      return;
    } else {
      console.log('Signing of' + configuration.path + ' failed with ' + JSON.stringify(result));
    }
  }

  throw Exception("Failed to sign " + configuration.path);
};
