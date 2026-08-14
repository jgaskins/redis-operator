pub mod redis;
pub mod redis_cluster;

pub use redis::Redis;
pub use redis_cluster::RedisCluster;

use kube::CustomResourceExt;

pub fn render() -> anyhow::Result<String> {
    let docs = [
        serde_yaml::to_string(&Redis::crd())?,
        serde_yaml::to_string(&RedisCluster::crd())?,
    ];
    let mut out = String::new();
    for doc in docs {
        out.push_str("---\n");
        out.push_str(&doc);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn deploy_crds_yaml_is_in_sync() {
        let expected = super::render().expect("render CRDs");
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/crds.yaml");
        let actual = std::fs::read_to_string(path)
            .expect("read deploy/crds.yaml — run `make gen-crds` to create it");
        assert_eq!(
            actual, expected,
            "deploy/crds.yaml is out of sync with the CRD types — run `make gen-crds`",
        );
    }
}
