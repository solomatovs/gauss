GRAFANA_DIR     := ../grafana
GRAFANA_PLUGINS := $(GRAFANA_DIR)/data/plugins
PLUGIN_NAME     := quotes-datasource
PLUGIN_BUILD    := bins/grafana-datasource/build
SERVER_BUILD    := build

.PHONY: help dev build-plugin deploy-plugin restart-grafana plugin \
        build-server server all clean-plugin

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Local Dev ──────────────────────────────────────────────────

dev: ## Build and run server locally with debug config
	cargo build && cargo run -p gauss-server -- serve --config config.debug.toml

# ── Grafana Plugin ──────────────────────────────────────────────

build-plugin: ## Build the Grafana plugin (frontend + Rust backend)
	docker compose -f $(PLUGIN_BUILD)/docker-compose.yml build

deploy-plugin: ## Build and deploy the plugin to Grafana plugins dir
	docker compose -f $(PLUGIN_BUILD)/docker-compose.yml up --build
	@echo ""
	@echo "Plugin deployed to $(GRAFANA_PLUGINS)/$(PLUGIN_NAME)"

restart-grafana: ## Restart the Grafana container to pick up plugin changes
	docker compose -f $(GRAFANA_DIR)/docker-compose.yml restart grafana
	@echo "Grafana restarted. Waiting for startup..."
	@sleep 3
	@docker compose -f $(GRAFANA_DIR)/docker-compose.yml logs --tail=20 grafana

plugin: deploy-plugin restart-grafana ## Build, deploy plugin and restart Grafana (full cycle)
	@echo ""
	@echo "Done! Plugin $(PLUGIN_NAME) is deployed and Grafana is restarted."

# ── Gauss Server ────────────────────────────────────────────────

build-server: ## Build the gauss-server image
	docker compose -f $(SERVER_BUILD)/docker-compose.yml build

server: build-server ## Build and extract server artifacts
	docker compose -f $(SERVER_BUILD)/docker-compose.yml up --build

# ── All ─────────────────────────────────────────────────────────

all: server plugin ## Build everything (server + plugin + deploy to Grafana)

# ── Cleanup ─────────────────────────────────────────────────────

clean-plugin: ## Remove the plugin from Grafana plugins dir
	rm -rf $(GRAFANA_PLUGINS)/$(PLUGIN_NAME)
	@echo "Plugin removed from Grafana"
