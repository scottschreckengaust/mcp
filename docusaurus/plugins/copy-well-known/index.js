const path = require('path');
const fs = require('fs-extra');

module.exports = function (context) {
  return {
    name: 'copy-well-known',
    async postBuild({outDir}) {
      const sourceFile = path.join(context.siteDir, 'static', '.well-known', 'mcp-registry-path');
      const destFile = path.join(outDir, '.well-known', 'mcp-registry-path');
      
      if (await fs.pathExists(sourceFile)) {
        await fs.ensureDir(path.dirname(destFile));
        await fs.copy(sourceFile, destFile);
        console.log('Copied .well-known/mcp-registry-path');
      }
    },
  };
};
