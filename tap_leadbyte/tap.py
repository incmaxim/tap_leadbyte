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
            default="v1.2",
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
