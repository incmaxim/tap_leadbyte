"""LeadByte tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_leadbyte import streams


class TapLeadByte(Tap):
    """LeadByte tap class."""

    name = "tap-leadbyte"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The API key for LeadByte API",
        ),
        th.Property(
            "domain",
            th.StringType,
            default="casesondemand",
            description="Your LeadByte domain (e.g., 'casesondemand' for casesondemand.leadbyte.com)",
        ),
        th.Property(
            "api_version",
            th.StringType,
            default="v1.3",
            description="API version to use (e.g., 'v1.2', 'v1.3')",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync (defaults to current time)",
        ),
        th.Property(
            "campaign_ids",
            th.ArrayType(th.StringType),
            description="List of campaign IDs to filter by (optional)",
        ),
        th.Property(
            "supplier_ids",
            th.ArrayType(th.StringType),
            description="List of supplier IDs to filter by (optional)",
        ),
        th.Property(
            "responder_ids",
            th.ArrayType(th.StringType),
            description="List of responder IDs to filter by (optional)",
        ),
        th.Property(
            "buyer_ids",
            th.ArrayType(th.StringType),
            description="List of buyer IDs to filter by (optional)",
        ),
        # Report-specific options
        th.Property(
            "debug",
            th.BooleanType,
            default=False,
            description="Beautify JSON output (Yes) or compact output (No)",
        ),
        th.Property(
            "group_by",
            th.StringType,
            description="Group reports by: Campaign, Day, Week, Month, Hour",
        ),
        th.Property(
            "date_preset",
            th.StringType,
            description="Date preset: today, yesterday, this_week, last_7d, lastweek, last_30d, this_month, last_month",
        ),
        # Supplier report options
        th.Property(
            "include_non_supplier_leads",
            th.BooleanType,
            default=False,
            description="Include leads which are not associated to a supplier",
        ),
        th.Property(
            "lead_type_api",
            th.BooleanType,
            default=True,
            description="Include API leads (REST/Submit APIs)",
        ),
        th.Property(
            "lead_type_import",
            th.BooleanType,
            default=False,
            description="Include imported leads",
        ),
        # Display options
        th.Property(
            "show_supplier",
            th.BooleanType,
            default=False,
            description="Show supplier information in reports",
        ),
        th.Property(
            "show_buyer",
            th.BooleanType,
            default=False,
            description="Show buyer information in reports",
        ),
        th.Property(
            "show_ssid",
            th.BooleanType,
            default=False,
            description="Show sub-supplier information in reports",
        ),
        th.Property(
            "show_campaign",
            th.BooleanType,
            default=False,
            description="Show campaign information when grouping by Day/Week/Month/Hour",
        ),
        # Master data options
        th.Property(
            "campaign_status",
            th.StringType,
            description="Filter campaigns by status: Active, Inactive, Archived",
        ),
        th.Property(
            "delivery_status",
            th.StringType,
            description="Filter deliveries by status: Active, Inactive, Saved",
        ),
        th.Property(
            "buyer_status",
            th.StringType,
            description="Filter buyers by status: Active, Inactive",
        ),
        # Lead activity options
        th.Property(
            "show_data",
            th.StringType,
            description="Show specific lead status counts: leads, valid, invalid, rejected, approved, returns",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.LeadByteStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EmailReportsStream(self),
            streams.SmsReportsStream(self),
            streams.BulkEmailReportsStream(self),
            streams.BulkSmsReportsStream(self),
            streams.SupplierReportsStream(self),
            streams.BuyerReportsStream(self),
            streams.CampaignReportsStream(self),
            streams.LeadActivityReportsStream(self),
            streams.CampaignsStream(self),
            streams.DeliveriesStream(self),
            streams.RespondersStream(self),
            streams.BuyersStream(self),
        ]


if __name__ == "__main__":
    TapLeadByte.cli()
