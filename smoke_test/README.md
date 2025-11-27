# smoke_test

A data engineering project built with [Odibi](https://github.com/henryodibi11/Odibi).

## Getting Started

### Prerequisites
- Python 3.9+
- Odibi (`pip install odibi`)

### Project Structure
- `odibi.yaml`: Main pipeline configuration
- `data/`: Local data storage
- `stories/`: Execution reports (HTML)

### Commands

**1. Validate Configuration**
```bash
odibi validate odibi.yaml
```

**2. Check Health**
```bash
odibi doctor
```

**3. Run Pipeline**
```bash
odibi run odibi.yaml
```

**4. View Results**
```bash
odibi ui
```

## CI/CD
A GitHub Actions workflow is included in `.github/workflows/ci.yaml` that validates the project on every push.
