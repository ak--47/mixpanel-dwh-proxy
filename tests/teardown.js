const kill = require('tree-kill');
const { promisify } = require('util');

module.exports = async () => {
    console.log('\n\nStopping test server...\n\n');
    const killAsync = promisify(kill);
    
    const promises = [];

    if (global.__SERVER__ && global.__SERVER__.pid) {
        promises.push(
            killAsync(global.__SERVER__.pid)
                .then(() => console.log('Test server stopped.\n\n'))
                .catch(error => console.error('Error stopping test server:', error))
        );
    }

    if (global.__FRONTEND__ && global.__FRONTEND__.pid) {
        promises.push(
            killAsync(global.__FRONTEND__.pid)
                .then(() => console.log('Frontend stopped.\n\n'))
                .catch(error => console.error('Error stopping frontend:', error))
        );
    }

    await Promise.all(promises);
    console.log('Teardown complete.\n');
};
