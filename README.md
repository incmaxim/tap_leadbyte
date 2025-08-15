# tap-leadbyte

`tap-leadbyte` is a Singer tap for LeadByte API.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from PyPi:

```bash
pipx install tap-leadbyte
```

Install from GitHub:

```bash
pipx install git+https://github.com/your-username/tap_leadbyte.git@main
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-leadbyte --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config keys and values will be retrieved from the environment. For example:

```bash
export TAP_LEADBYTE_API_KEY="your-api-key"
export TAP_LEADBYTE_START_DATE="2023-01-01T00:00:00Z"
```

### Source Authentication and Authorization

The LeadByte API requires an API key for authentication. You can obtain your API key from your LeadByte account dashboard.

## Usage

You can learn about the data your source application has available using `tap-leadbyte --about --format=json`.

### Executing the Tap Directly

```bash
tap-leadbyte --version
tap-leadbyte --help
tap-leadbyte --config CONFIG --discover > ./catalog.json
```

## Configuration Parameters

- `api_key` (required): Your LeadByte API key
- `domain` (optional): Your LeadByte domain (defaults to "casesondemand")
- `api_version` (optional): API version (defaults to "v1.3")
- `start_date` (required): Start date for data extraction in ISO 8601 format
- `end_date` (optional): End date for data extraction (defaults to current time)
- `campaign_ids` (optional): List of campaign IDs to filter by
- `supplier_ids` (optional): List of supplier IDs to filter by
- `responder_ids` (optional): List of responder IDs to filter by  
- `buyer_ids` (optional): List of buyer IDs to filter by

## Available Streams

This tap extracts data from the following LeadByte API endpoints:

### Reports
- `email_reports` - Email campaign summary reports
- `sms_reports` - SMS campaign summary reports  
- `bulk_email_reports` - Bulk email campaign summary reports
- `bulk_sms_reports` - Bulk SMS campaign summary reports
- `supplier_reports` - Supplier summary reports
- `buyer_reports` - Buyer summary reports
- `campaign_reports` - Campaign summary reports
- `lead_activity_reports` - Lead activity reports

### Master Data
- `campaigns` - Campaign master data
- `deliveries` - Delivery configurations
- `responders` - Responder configurations with pushes
- `buyers` - Buyer master data

## Example Configuration

```json
{
  "api_key": "your-api-key-here",
  "domain": "casesondemand",
  "api_version": "v1.3",
  "start_date": "2023-01-01T00:00:00Z",
  "end_date": "2023-12-31T23:59:59Z",
  "campaign_ids": ["all"]
}
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-leadbyte` CLI interface directly using `poetry run`:

```bash
poetry run tap-leadbyte --help
```

## License

This project is licensed under the Apache 2.0 License.
