---
name: setup mcp config
description: Create the .mcp.json file to connect Claude Code to the DataSurface MCP server running in Kubernetes.
---
# Setup DataSurface MCP Config

Create a `.mcp.json` file in the project root so Claude Code can connect to the DataSurface MCP server running in the Kubernetes cluster.

## Steps

### 1. Find the MCP Service

Look up the MCP service in the target namespace to get the NodePort:

```bash
kubectl get service -n <namespace> -l app=demo-psp-mcp-server
```

The service named `demo-psp-mcp` exposes port 8000 via a NodePort. Note the NodePort value (e.g., `32132`).

### 2. Verify the MCP Server is Running

```bash
kubectl get pods -n <namespace> -l app=demo-psp-mcp-server
```

Confirm the pod is in `Running` state.

### 3. Create `.mcp.json`

Write the following `.mcp.json` file to the project root, using the NodePort discovered in step 1:

```json
{
  "mcpServers": {
    "datasurface": {
      "type": "sse",
      "url": "http://localhost:<nodeport>/sse"
    }
  }
}
```

Replace `<nodeport>` with the actual NodePort value from step 1.

### 4. Verify Settings

Check that `.claude/settings.local.json` has the MCP server enabled. It should contain:

```json
{
  "enableAllProjectMcpServers": true,
  "enabledMcpjsonServers": ["datasurface"]
}
```

If these settings are missing, add them.

### 5. Inform the User

Tell the user to restart Claude Code (or start a new session) for the MCP server connection to take effect.

## Notes

- The MCP server uses SSE (Server-Sent Events) transport with endpoints at `/sse` and `/messages/`
- The server runs inside Kubernetes and is exposed via a NodePort service
- The default namespace is `demo1` but should be confirmed with the user
