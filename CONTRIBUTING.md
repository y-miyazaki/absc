# Contributing to ABSC

Thank you for your interest in contributing to ABSC! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Adding New Service Collectors](#adding-new-service-collectors)

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a new branch for your changes
4. Make your changes
5. Test your changes
6. Submit a pull request

## Development Setup

### Prerequisites

- Go 1.25.8 or later
- AWS credentials configured
- golangci-lint (for linting)

### Clone and Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/absc.git
cd absc

# Add upstream remote
git remote add upstream https://github.com/y-miyazaki/absc.git

# Install dependencies
go mod download

# Install development tools
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

## Development Workflow

### Create a Feature Branch

```bash
# Update your local main/develop branch
git checkout develop
git pull upstream develop

# Create a new feature branch
git checkout -b feature/your-feature-name
```

### Make Changes

1. Write your code following the [Code Style](#code-style) guidelines
2. Add or update tests as needed
3. Update documentation if necessary
4. Run tests and linting locally

### Commit Your Changes

```bash
# Stage your changes
git add .

# Commit with a descriptive message
git commit -m "feat: add support for AWS Service X

- Implement collector for Service X
- Add tests for Service X collector
- Update documentation"
```

## Testing

### Run All Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Run Specific Tests

```bash
# Run tests for a specific package
go test ./internal/aws/resources/

# Run a specific test function
go test -run TestEC2Collector ./internal/aws/resources/
```

### Testing with Real AWS Resources

```bash
# Set environment variables
export AWS_PROFILE=your-test-profile
export AWS_DEFAULT_REGION=ap-northeast-1

# Run the tool
go run cmd/absc/main.go -c ec2,s3 -v
```

## Code Style

### Go Code Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Use `gofmt` for formatting (automatically applied by golangci-lint)
- Follow naming conventions:
  - PascalCase for exported functions, types, and constants
  - camelCase for unexported functions and variables
  - snake_case for file names
- Write clear, descriptive comments for exported functions
- Handle errors explicitly - do not ignore errors with `_`

### Running Linters

```bash
# Run golangci-lint
golangci-lint run

# Run with auto-fix
golangci-lint run --fix

# Run specific linters
golangci-lint run --enable-only=errcheck,staticcheck
```

### Code Documentation

- Add package-level documentation at the top of each package
- Document all exported functions, types, and constants
- Use GoDoc format for comments
- Include examples in documentation when helpful

Example:

```go
// Package resources provides collectors for AWS resources.
package resources

// EC2Collector collects EC2 resources including instances, VPCs, and subnets
type EC2Collector struct{}

// Name returns the resource name of the collector.
func (*EC2Collector) Name() string {
    return "ec2"
}
```

## Submitting Changes

### Before Submitting

1. Ensure all tests pass: `go test ./...`
2. Run linters: `golangci-lint run`
3. Update documentation if needed
4. Rebase your branch on the latest develop branch

```bash
git fetch upstream
git rebase upstream/develop
```

### Create a Pull Request

1. Push your changes to your fork

```bash
git push origin feature/your-feature-name
```

2. Go to the GitHub repository and create a pull request
3. Fill in the pull request template with:
   - Clear description of changes
   - Related issue numbers (if any)
   - Testing performed
   - Screenshots (if applicable)

### Pull Request Review Process

- Maintainers will review your pull request
- Address any feedback or requested changes
- Once approved, your changes will be merged

## Adding New Service Collectors

See [docs/02_implementation_guide.md](docs/02_implementation_guide.md) for detailed guidelines on implementing new AWS service collectors.

### Quick Overview

1. Create a new file in `internal/aws/resources/` (e.g., `servicename.go`)
2. Implement the collector interface
3. Add tests in `servicename_test.go`
4. Register the collector in the main collection logic
5. Update documentation

Example structure:

```go
package resources

// ServiceNameCollector collects resources from AWS ServiceName.
type ServiceNameCollector struct{}

// Name returns the resource name of the collector.
func (*ServiceNameCollector) Name() string {
    return "servicename"
}

// ShouldSort returns whether the collected resources should be sorted.
func (*ServiceNameCollector) ShouldSort() bool {
    return true
}

// GetColumns returns the CSV columns for the collector.
func (*ServiceNameCollector) GetColumns() []Column {
    return []Column{
        {Name: "Category", Sort: false},
        {Name: "SubCategory", Sort: false},
        {Name: "SubSubCategory", Sort: false},
        {Name: "Name", Sort: true},
        {Name: "Region", Sort: false},
        // Add service-specific columns here
    }
}

// Collect retrieves all ServiceName resources.
func (c *ServiceNameCollector) Collect(ctx context.Context, cfg *aws.Config, region string) ([]Resource, error) {
    // Implementation
    return []Resource{}, nil
}
```

## Project Structure

```
.
├── cmd/absc/              # Main application entry point
├── internal/
│   ├── aws/             # AWS client and resource collectors
│   │   ├── resources/   # Service-specific collectors
│   │   └── helpers/     # Helper functions
│   ├── exporter/        # Output formatters (CSV, HTML)
│   ├── logger/          # Logging utilities
│   └── validation/      # Input validation
├── docs/                # Documentation
├── scripts/             # Build and utility scripts
└── test/                # Test helpers and fixtures
```

## Release Process

### For Maintainers

ABSC uses [GoReleaser](https://goreleaser.com/) with GitHub Actions for automated releases.

#### Creating a New Release

1. **Prepare the release**
   - Ensure all changes are merged to `develop` branch
   - Update version in relevant files if needed
   - Review and update CHANGELOG

2. **Trigger the release workflow**
   - Go to GitHub Actions → "cd-wd-go-releaser" workflow
   - Click "Run workflow"
   - Fill in the parameters:
     - **version**: Release version (e.g., `v1.0.1`, `v1.1.0-beta.1`)
     - **draft**: Create as draft (recommended for review)
     - **prerelease**: Mark as prerelease (for beta/rc versions)
     - **create_tag**: Automatically create git tag (default: true)
     - **environment**: Target environment (default: prd)

3. **Verify the release**
   - Check the workflow execution logs
   - Review the created release on GitHub
   - Test the release binaries

#### Release Workflow Features

- **Automatic builds** for multiple platforms:
  - Linux (amd64, arm64)
  - macOS (amd64, arm64)
  - Windows (amd64)
- **Automatic changelog** generation from git commits
- **Checksum file** generation for integrity verification
- **GitHub release** creation with all artifacts

#### Manual Release Using GoReleaser

If you prefer manual releases:

```bash
# Install aqua tools (including goreleaser)
aqua i -l

# Test release locally (creates snapshot)
goreleaser release --snapshot --clean

# Create a git tag
git tag -a v1.0.1 -m "Release v1.0.1"
git push origin v1.0.1

# Run goreleaser (requires GITHUB_TOKEN)
export GITHUB_TOKEN="your-github-token"
goreleaser release --clean
```

#### Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Incompatible API changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

Examples:
- `v1.0.1` - Stable release
- `v1.1.0-beta.1` - Beta release
- `v1.1.0-rc.1` - Release candidate

## Questions?

- 📖 Check the [Documentation](docs/)
- 🐛 Open an [Issue](https://github.com/y-miyazaki/absc/issues)
- 💬 Start a [Discussion](https://github.com/y-miyazaki/absc/discussions)

Thank you for contributing to ABSC!
