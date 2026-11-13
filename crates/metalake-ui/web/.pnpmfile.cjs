// TypeScript 7, the native compiler, has no JavaScript API; openapi-typescript writes the API's types through
// TypeScript 5's, which it asks for as a peer. It is given its own, apart from the one that checks this package.
module.exports = {
  hooks: {
    readPackage(pkg) {
      if (pkg.name === "openapi-typescript") {
        delete pkg.peerDependencies?.typescript;
        pkg.dependencies = { ...pkg.dependencies, typescript: "^5.9.0" };
      }
      return pkg;
    },
  },
};
