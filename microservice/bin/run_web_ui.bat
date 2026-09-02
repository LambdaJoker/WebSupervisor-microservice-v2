@echo off
cd /d "%~dp0..\web-ui-service"
cargo run -- -config_path .\config.json
