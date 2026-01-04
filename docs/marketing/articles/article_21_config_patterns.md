# Configuration Patterns for Multi-Environment Pipelines

*Same pipeline, different environments*

---

## TL;DR

Production pipelines run in multiple environments: dev, staging, production. Each needs different connection strings, credentials, and behaviors. This article covers patterns for managing environment-specific configuration: variable substitution, environment overrides, and secret management.

---

## The Problem: Hardcoded Configs

```yaml
# ❌ Works in dev, breaks everywhere else
connections:
  bronze:
    type: azure_blob
    account: devstorageaccount123
    container: bronze-dev
    credential: abc123secret  # Committed to git!
```

Problems:
- Different storage accounts per environment
- Secrets in version control
- Manual changes for each deployment

---

## Pattern 1: Environment Variables

Use `${VAR}` syntax for environment-specific values:

```yaml
connections:
  bronze:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    container: ${BRONZE_CONTAINER}
    credential: ${STORAGE_KEY}
```

Set differently per environment:

```bash
# Development
export STORAGE_ACCOUNT=devstorageaccount
export BRONZE_CONTAINER=bronze-dev
export STORAGE_KEY=dev-key-123

# Production
export STORAGE_ACCOUNT=prodstorageaccount
export BRONZE_CONTAINER=bronze-prod
export STORAGE_KEY=prod-key-secure
```

---

## Pattern 2: Environment Overrides

Define base config with environment-specific overrides:

```yaml
# Base configuration
project: "ecommerce_warehouse"
engine: "pandas"

connections:
  bronze:
    type: local
    base_path: "./bronze"

# Environment overrides
environments:
  dev:
    connections:
      bronze:
        base_path: "./dev/bronze"
  
  staging:
    connections:
      bronze:
        type: azure_blob
        account: ${STAGING_ACCOUNT}
        container: bronze-staging
  
  production:
    engine: "spark"  # Use Spark in prod
    connections:
      bronze:
        type: azure_blob
        account: ${PROD_ACCOUNT}
        container: bronze-prod
```

Run with environment flag:

```bash
odibi run odibi.yaml --env production
```

---

## Pattern 3: Global Variables

Define reusable variables:

```yaml
vars:
  project_name: "ecommerce"
  team: "data-platform"
  retention_days: 90

connections:
  bronze:
    base_path: "./${vars.project_name}/bronze"

pipelines:
  - pipeline: bronze_${vars.project_name}
    description: "Bronze layer for ${vars.project_name}"
```

Override per environment:

```yaml
environments:
  production:
    vars:
      retention_days: 365  # Keep longer in prod
```

---

## Pattern 4: Secret Management

Never store secrets in YAML. Reference them:

### Azure Key Vault

```yaml
connections:
  bronze:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    credential:
      type: keyvault
      vault: ${KEYVAULT_NAME}
      secret: storage-key
```

### Environment Variables

```yaml
connections:
  bronze:
    credential: ${STORAGE_KEY}  # Set in environment
```

### Databricks Secrets

```yaml
connections:
  bronze:
    credential:
      type: databricks_secret
      scope: data-platform
      key: storage-key
```

---

## Pattern 5: Separate Config Files

Split configuration by concern:

```
config/
├── odibi.yaml           # Main entry point
├── connections.yaml     # Connection definitions
├── pipelines/
│   ├── bronze.yaml      # Bronze pipeline
│   ├── silver.yaml      # Silver pipeline
│   └── gold.yaml        # Gold pipeline
└── environments/
    ├── dev.yaml         # Dev overrides
    ├── staging.yaml     # Staging overrides
    └── prod.yaml        # Prod overrides
```

Main config references others:

```yaml
# odibi.yaml
project: "ecommerce"
engine: "pandas"

include:
  - connections.yaml
  - pipelines/bronze.yaml
  - pipelines/silver.yaml
  - pipelines/gold.yaml
```

---

## Pattern 6: Feature Flags

Enable/disable features per environment:

```yaml
vars:
  enable_quality_checks: true
  enable_notifications: false
  sample_data: false

pipelines:
  - pipeline: silver
    nodes:
      - name: silver_orders
        enabled: ${vars.enable_quality_checks}
        contracts:
          # ...

environments:
  dev:
    vars:
      enable_quality_checks: false  # Skip in dev
      sample_data: true             # Use sample data
  
  production:
    vars:
      enable_quality_checks: true
      enable_notifications: true
```

---

## Pattern 7: Conditional Node Execution

Enable nodes based on environment:

```yaml
- name: send_slack_notification
  enabled: ${vars.enable_notifications}
  # Only runs in production

- name: sample_data
  enabled: ${vars.sample_data}
  # Only runs in dev
```

Or use tags:

```yaml
- name: expensive_validation
  tags: [production]
  # Run: odibi run --tag production
```

---

## Complete Multi-Environment Setup

```yaml
# odibi.yaml

project: "ecommerce_warehouse"
engine: ${ENGINE}
version: "1.0.0"

# Global variables
vars:
  env: ${ENVIRONMENT:-dev}
  retention_days: 30
  enable_quality_checks: true
  enable_notifications: false

# Connections with variable substitution
connections:
  landing:
    type: ${LANDING_TYPE:-local}
    base_path: ${LANDING_PATH:-./data/landing}
  
  bronze:
    type: ${BRONZE_TYPE:-local}
    base_path: ${BRONZE_PATH:-./bronze}
  
  silver:
    type: ${SILVER_TYPE:-local}
    base_path: ${SILVER_PATH:-./silver}
  
  gold:
    type: ${GOLD_TYPE:-local}
    base_path: ${GOLD_PATH:-./gold}

# System config
system:
  connection: bronze
  path: _system

story:
  connection: bronze
  path: _stories
  retention_days: ${vars.retention_days}

# Environment-specific overrides
environments:
  dev:
    engine: pandas
    vars:
      enable_quality_checks: false
  
  staging:
    engine: spark
    connections:
      landing:
        type: azure_blob
        account: ${STAGING_STORAGE}
        container: landing
        credential: ${STAGING_KEY}
      bronze:
        type: azure_blob
        account: ${STAGING_STORAGE}
        container: bronze
        credential: ${STAGING_KEY}
  
  production:
    engine: spark
    vars:
      retention_days: 90
      enable_quality_checks: true
      enable_notifications: true
    connections:
      landing:
        type: azure_blob
        account: ${PROD_STORAGE}
        container: landing
        credential:
          type: keyvault
          vault: ${KEYVAULT_NAME}
          secret: storage-key
      bronze:
        type: azure_blob
        account: ${PROD_STORAGE}
        container: bronze
        credential:
          type: keyvault
          vault: ${KEYVAULT_NAME}
          secret: storage-key
```

---

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/deploy.yaml
name: Deploy Pipeline

on:
  push:
    branches: [main]

jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v2
      
      - name: Run Pipeline
        env:
          ENVIRONMENT: staging
          STAGING_STORAGE: ${{ secrets.STAGING_STORAGE }}
          STAGING_KEY: ${{ secrets.STAGING_KEY }}
        run: |
          odibi run odibi.yaml --env staging

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v2
      
      - name: Run Pipeline
        env:
          ENVIRONMENT: production
          PROD_STORAGE: ${{ secrets.PROD_STORAGE }}
          KEYVAULT_NAME: ${{ secrets.KEYVAULT_NAME }}
        run: |
          odibi run odibi.yaml --env production
```

### Azure DevOps

```yaml
# azure-pipelines.yaml
stages:
  - stage: Staging
    variables:
      - group: staging-variables
    jobs:
      - job: RunPipeline
        steps:
          - script: |
              odibi run odibi.yaml --env staging
            env:
              STAGING_STORAGE: $(STAGING_STORAGE)
              STAGING_KEY: $(STAGING_KEY)

  - stage: Production
    dependsOn: Staging
    variables:
      - group: production-variables
    jobs:
      - job: RunPipeline
        steps:
          - script: |
              odibi run odibi.yaml --env production
```

---

## Validation

Validate configuration before running:

```bash
# Check config syntax
odibi validate odibi.yaml

# Check with environment
odibi validate odibi.yaml --env production

# Check variables are set
odibi validate odibi.yaml --env production --check-vars
```

Output:

```
✓ YAML syntax valid
✓ All nodes have unique names
✓ All dependencies exist
✓ All connections defined
✗ Missing variable: PROD_STORAGE
✗ Missing variable: KEYVAULT_NAME
```

---

## Key Takeaways

| Pattern | Use Case |
|---------|----------|
| Environment variables | Secrets, account names |
| Environment overrides | Engine, paths, feature flags |
| Global variables | Shared values, DRY |
| Secret management | Never store secrets in YAML |
| Separate files | Large configurations |
| Feature flags | Environment-specific behavior |

---

## Next Steps

With configuration solid, let's cover debugging when things go wrong:

- Log analysis
- Common error patterns
- Step-by-step debugging

Next article: **Debugging Data Pipelines: A Systematic Approach**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
