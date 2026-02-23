mod plugin;

use plugin::QuotesPlugin;

#[grafana_plugin_sdk::main(
    services(data, diagnostics, stream, resource),
    init_subscriber = true,
)]
async fn plugin() -> QuotesPlugin {
    QuotesPlugin::new()
}
