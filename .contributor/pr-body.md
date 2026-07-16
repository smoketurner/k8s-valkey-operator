Fixes #158

This change ensures that when the read service is disabled (spec.read_service.enabled = false), the corresponding Kubernetes Service is deleted to prevent TLS hostname verification failures.

The deletion logic is added after the existing read service conditional patch block, ignoring 404 errors (service already gone).

---

*This change was made using Claude Code (Anthropic's AI coding assistant).*
