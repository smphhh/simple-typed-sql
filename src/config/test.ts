
export function createConfig() {
    return {
        knexConnection: {
            client: 'pg',
            connection: {
                user: 'samuli',
                password: 'samuli',
                host: '10.0.75.235',
                port: 5433,
                database: 'gio_etl_scheduler_test1'
            },
            pool: {
                min: 0,
                max: 3
            }
        }
    };
}
