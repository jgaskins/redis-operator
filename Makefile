.PHONY: gen-crds check-crds test

gen-crds:
	cargo run -q -- crds > deploy/crds.yaml

check-crds:
	cargo test -q deploy_crds_yaml_is_in_sync

test:
	cargo test
