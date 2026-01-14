//! RAII namespace management for isolated tests.
//!
//! Each test can create its own namespace that is automatically
//! cleaned up when the test completes.
//!
//! IMPORTANT: Tests using TestNamespace must use `#[tokio::test(flavor = "multi_thread")]`
//! to support synchronous cleanup via `block_in_place`.

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DeleteParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::{Client, Resource};
use my_operator::crd::MyResource;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

/// A test namespace that is automatically deleted when dropped.
///
/// Use this to create isolated namespaces for each test to prevent
/// interference between tests running in parallel.
///
/// IMPORTANT: Tests using this must use `#[tokio::test(flavor = "multi_thread")]`
pub struct TestNamespace {
    client: Client,
    name: String,
    /// Track if cleanup has already been initiated
    cleanup_initiated: AtomicBool,
}

impl TestNamespace {
    /// Create a new test namespace with a unique suffix.
    ///
    /// The namespace name will be `{prefix}-{uuid}` to ensure uniqueness.
    pub async fn create(client: Client, prefix: &str) -> Self {
        let suffix = Uuid::new_v4().to_string()[..8].to_string();
        let name = format!("{}-{}", prefix, suffix);

        let ns_api: Api<Namespace> = Api::all(client.clone());

        let ns = Namespace {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                labels: Some(
                    [(
                        "app.kubernetes.io/managed-by".to_string(),
                        "integration-test".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        };

        ns_api
            .create(&PostParams::default(), &ns)
            .await
            .unwrap_or_else(|e| panic!("Failed to create test namespace {}: {}", name, e));

        tracing::info!(namespace = %name, "Created test namespace");

        Self {
            client,
            name,
            cleanup_initiated: AtomicBool::new(false),
        }
    }

    /// Get the name of the test namespace.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get an API client for resources in this namespace.
    pub fn api<K>(&self) -> Api<K>
    where
        K: kube::Resource<Scope = kube::core::NamespaceResourceScope>,
        <K as kube::Resource>::DynamicType: Default,
    {
        Api::namespaced(self.client.clone(), &self.name)
    }

    /// Delete the namespace (called automatically on drop).
    async fn delete(&self) {
        let ns_api: Api<Namespace> = Api::all(self.client.clone());

        match ns_api.delete(&self.name, &DeleteParams::default()).await {
            Ok(_) => {
                tracing::info!(namespace = %self.name, "Deleted test namespace");
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Already deleted, ignore
            }
            Err(e) => {
                tracing::warn!(namespace = %self.name, error = %e, "Failed to delete test namespace");
            }
        }
    }
}

/// Automatic cleanup on drop - synchronously deletes resources and namespace.
///
/// Uses tokio::task::block_in_place to allow blocking on async code from within
/// the tokio runtime. This requires the multi-threaded runtime (use `#[tokio::test(flavor = "multi_thread")]`).
impl Drop for TestNamespace {
    fn drop(&mut self) {
        // Check if cleanup was already initiated
        if self.cleanup_initiated.swap(true, Ordering::SeqCst) {
            return;
        }

        let name = self.name.clone();
        let client = self.client.clone();

        tracing::debug!("Drop: cleaning up namespace {}", name);

        // Use block_in_place to allow blocking from within the async runtime
        // This requires the multi-threaded runtime
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                // First delete all MyResource resources to trigger operator cleanup
                Self::delete_resources::<MyResource>(&client, &name).await;

                // Brief wait for operator to process deletions
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Remove any remaining finalizers
                Self::remove_resource_finalizers::<MyResource>(&client, &name).await;

                let ns_api: Api<Namespace> = Api::all(client);
                let dp = DeleteParams {
                    propagation_policy: Some(kube::api::PropagationPolicy::Background),
                    ..Default::default()
                };

                match ns_api.delete(&name, &dp).await {
                    Ok(_) => {
                        tracing::debug!("Drop: namespace {} deletion initiated", name);
                    }
                    Err(kube::Error::Api(e)) if e.code == 404 => {
                        tracing::debug!("Drop: namespace {} already deleted", name);
                    }
                    Err(e) => {
                        tracing::warn!("Drop: failed to delete namespace {}: {}", name, e);
                    }
                }
            });
        });
    }
}

impl TestNamespace {
    /// Delete all resources of type `T` in the namespace.
    async fn delete_resources<T>(client: &Client, namespace: &str)
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope> + Clone + DeserializeOwned + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let dt = T::DynamicType::default();
        let kind = T::kind(&dt);
        let api: Api<T> = Api::namespaced(client.clone(), namespace);

        let resource_list = match api.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list {} resources for deletion: {}", kind, e);
                return;
            }
        };

        let dp = DeleteParams::default();
        for resource in resource_list.items {
            if let Some(name) = Resource::meta(&resource).name.as_ref()
                && let Err(e) = api.delete(name, &dp).await
            {
                tracing::debug!("Failed to delete {} {}: {}", kind, name, e);
            }
        }
    }

    /// Remove finalizers from all resources of type `T` in the namespace.
    async fn remove_resource_finalizers<T>(client: &Client, namespace: &str)
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope>
            + Clone
            + DeserializeOwned
            + Serialize
            + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let dt = T::DynamicType::default();
        let kind = T::kind(&dt);
        let api: Api<T> = Api::namespaced(client.clone(), namespace);

        let resource_list = match api.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list {} resources for cleanup: {}", kind, e);
                return;
            }
        };

        let patch: Patch<serde_json::Value> =
            Patch::Merge(json!({"metadata": {"finalizers": null}}));
        let patch_params = PatchParams::default();

        for resource in resource_list.items {
            let meta = Resource::meta(&resource);
            if let Some(name) = meta.name.as_ref() {
                // Skip if no finalizers
                if meta.finalizers.as_ref().is_none_or(|f| f.is_empty()) {
                    continue;
                }
                if let Err(e) = api.patch(name, &patch_params, &patch).await {
                    tracing::warn!("Failed to remove finalizer from {} {}: {}", kind, name, e);
                } else {
                    tracing::debug!("Removed finalizer from {} {}", kind, name);
                }
            }
        }
    }
}

/// Wait for a namespace to be fully deleted.
#[allow(dead_code)]
pub async fn wait_for_namespace_deletion(client: Client, name: &str, timeout: Duration) {
    use tokio::time::{Instant, sleep};

    let ns_api: Api<Namespace> = Api::all(client);
    let start = Instant::now();

    loop {
        match ns_api.get(name).await {
            Ok(_) => {
                if start.elapsed() > timeout {
                    panic!("Timeout waiting for namespace {} to be deleted", name);
                }
                sleep(Duration::from_millis(500)).await;
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                break;
            }
            Err(e) => {
                panic!("Error checking namespace {}: {}", name, e);
            }
        }
    }
}
