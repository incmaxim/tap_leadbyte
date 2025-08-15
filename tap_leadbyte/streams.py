"""Stream type classes for tap-leadbyte."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_leadbyte.client import LeadByteStream


class ReportsStream(LeadByteStream):
    """Base class for report streams."""
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for reports."""
        params = super().get_url_params(context, next_page_token)
        
        # Add date parameters
        params["from"] = self.config["start_date"].strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if self.config.get("end_date"):
            params["to"] = self.config["end_date"].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            params["to"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Add campaign IDs if specified
        if self.config.get("campaign_ids"):
            params["campaignId"] = ",".join(self.config["campaign_ids"])
        else:
            params["campaignId"] = "all"
            
        # Add supplier IDs if specified
        if self.config.get("supplier_ids"):
            params["supplierId"] = ",".join(self.config["supplier_ids"])
            
        # Add responder IDs if specified
        if self.config.get("responder_ids"):
            params["responderId"] = ",".join(self.config["responder_ids"])
            
        # Add buyer IDs if specified
        if self.config.get("buyer_ids"):
            params["buyerId"] = ",".join(self.config["buyer_ids"])
            
        return params


class EmailReportsStream(ReportsStream):
    """Email reports stream."""
    
    name = "email_reports"
    path = "/reports/email"
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]  # ✅ Fixed format
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("responder", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("push", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("advertiser", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("sent", th.StringType),
        th.Property("delivered", th.StringType),
        th.Property("opened", th.StringType),
        th.Property("clicks", th.StringType),
        th.Property("conversions", th.StringType),
        th.Property("bounced", th.StringType),
        th.Property("unsubscribed", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("revenue", th.StringType),
        th.Property("profit", th.StringType),
        th.Property("currency", th.StringType),
        # ✅ Adding flattened primary key fields for Singer compatibility
        th.Property("campaign_id", th.IntegerType, description="Derived from campaign.id"),
        th.Property("responder_id", th.IntegerType, description="Derived from responder.id"),  
        th.Property("supplier_id", th.IntegerType, description="Derived from supplier.id"),
        th.Property("push_id", th.IntegerType, description="Derived from push.id"),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add flattened primary key fields."""
        row["campaign_id"] = row["campaign"]["id"]
        row["responder_id"] = row["responder"]["id"]  
        row["supplier_id"] = row["supplier"]["id"]
        row["push_id"] = row["push"]["id"]
        return row


class SmsReportsStream(ReportsStream):
    """SMS reports stream."""
    
    name = "sms_reports"
    path = "/reports/sms"
    primary_keys = ["campaign.id", "responder.id", "supplier.id", "push.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("responder", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("push", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("redirect", th.StringType),
        )),
        th.Property("advertiser", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("sent", th.StringType),
        th.Property("pending", th.StringType),
        th.Property("undelivered", th.StringType),
        th.Property("delivered", th.StringType),
        th.Property("clicks", th.StringType),
        th.Property("conversions", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("revenue", th.StringType),
        th.Property("profit", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()


class BulkEmailReportsStream(ReportsStream):
    """Bulk email reports stream."""
    
    name = "bulk_email_reports"
    path = "/reports/bulkemail"
    primary_keys = ["campaign.id", "responder.id", "supplier.id", "push.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("responder", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("push", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("advertiser", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("sent", th.StringType),
        th.Property("delivered", th.StringType),
        th.Property("opened", th.StringType),
        th.Property("clicks", th.StringType),
        th.Property("conversions", th.StringType),
        th.Property("bounced", th.StringType),
        th.Property("unsubscribed", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("revenue", th.StringType),
        th.Property("profit", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()


class BulkSmsReportsStream(ReportsStream):
    """Bulk SMS reports stream."""
    
    name = "bulk_sms_reports"
    path = "/reports/bulksms"
    primary_keys = ["campaign.id", "responder.id", "supplier.id", "push.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("responder", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("push", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("redirect", th.StringType),
        )),
        th.Property("advertiser", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("sent", th.StringType),
        th.Property("pending", th.StringType),
        th.Property("undelivered", th.StringType),
        th.Property("delivered", th.StringType),
        th.Property("clicks", th.StringType),
        th.Property("conversions", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("revenue", th.StringType),
        th.Property("profit", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()


class SupplierReportsStream(ReportsStream):
    """Supplier reports stream."""
    
    name = "supplier_reports"
    path = "/reports/supplier"
    primary_keys = ["campaign.id", "supplier.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("leads", th.IntegerType),
        th.Property("valid", th.IntegerType),
        th.Property("invalid", th.IntegerType),
        th.Property("validCR", th.NumberType),
        th.Property("pending", th.IntegerType),
        th.Property("rejected", th.IntegerType),
        th.Property("payable", th.IntegerType),
        th.Property("sold", th.IntegerType),
        th.Property("returns", th.IntegerType),
        th.Property("payableCR", th.NumberType),
        th.Property("payout", th.NumberType),
        th.Property("emailCost", th.NumberType),
        th.Property("smsCost", th.NumberType),
        th.Property("validationCost", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("profit", th.NumberType),
        th.Property("eCPL", th.NumberType),
        th.Property("eRPL", th.NumberType),
        th.Property("payoutAdjusted", th.NumberType),
        th.Property("revenueAdjusted", th.NumberType),
        th.Property("profitAdjusted", th.NumberType),
        th.Property("eCPLAdjusted", th.NumberType),
        th.Property("eRPLAdjusted", th.NumberType),
        th.Property("currency", th.StringType),
    ).to_dict()


class BuyerReportsStream(ReportsStream):
    """Buyer reports stream."""
    
    name = "buyer_reports"
    path = "/reports/buyer"
    primary_keys = ["campaign.id", "buyer.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("buyer", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("bid", th.StringType),
        )),
        th.Property("posted", th.IntegerType),
        th.Property("accepted", th.IntegerType),
        th.Property("sold", th.IntegerType),
        th.Property("rejected", th.IntegerType),
        th.Property("approvedCR", th.NumberType),
        th.Property("returned", th.IntegerType),
        th.Property("returnedPercent", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("RPL", th.NumberType),
        th.Property("RPS", th.NumberType),
        th.Property("currency", th.StringType),
    ).to_dict()


class CampaignReportsStream(ReportsStream):
    """Campaign reports stream."""
    
    name = "campaign_reports"
    path = "/reports/campaign"
    primary_keys = ["campaign.id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("date", th.StringType),
        th.Property("leads", th.IntegerType),
        th.Property("valid", th.IntegerType),
        th.Property("invalid", th.IntegerType),
        th.Property("pending", th.IntegerType),
        th.Property("rejections", th.IntegerType),
        th.Property("payable", th.IntegerType),
        th.Property("sold", th.IntegerType),
        th.Property("returns", th.IntegerType),
        th.Property("payout", th.NumberType),
        th.Property("emailCost", th.NumberType),
        th.Property("smsCost", th.NumberType),
        th.Property("validationCost", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("profit", th.NumberType),
        th.Property("currency", th.StringType),
    ).to_dict()


class LeadActivityReportsStream(ReportsStream):
    """Lead activity reports stream."""
    
    name = "lead_activity_reports"
    path = "/reports/leadactivity"
    primary_keys = ["campaign.id", "date"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("sid", th.StringType),
        )),
        th.Property("date", th.StringType),
        th.Property("count", th.IntegerType),
    ).to_dict()


class CampaignsStream(LeadByteStream):
    """Campaigns stream."""
    
    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("description", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("country", th.StringType),
        th.Property("sms_field", th.StringType),
        th.Property("active", th.StringType),
        th.Property("sup_visible", th.StringType),
        th.Property("archived", th.StringType),
    ).to_dict()
    
    records_jsonpath = "$[*]"  # Campaigns returns array directly


class DeliveriesStream(LeadByteStream):
    """Deliveries stream."""
    
    name = "deliveries"
    path = "/deliveries"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("status", th.StringType),
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("deliver_to", th.StringType),
        th.Property("remote_system", th.ObjectType()),
    ).to_dict()
    
    records_jsonpath = "$.deliveries[*]"


class RespondersStream(LeadByteStream):
    """Responders stream."""
    
    name = "responders"
    path = "/responders"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("status", th.StringType),
        th.Property("campaign", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("suppression", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("reference", th.StringType),
        )),
        th.Property("supplier", th.StringType),
        th.Property("pause_from", th.StringType),
        th.Property("pause_to", th.StringType),
        th.Property("pushes", th.ArrayType(th.ObjectType(
            th.Property("push_id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("type", th.StringType),
            th.Property("advertiser", th.StringType),
            th.Property("marketing_category", th.StringType),
            th.Property("sent", th.StringType),
            th.Property("pending", th.IntegerType),
            th.Property("undelivered", th.IntegerType),
            th.Property("delivered", th.IntegerType),
            th.Property("opened", th.StringType),
            th.Property("clicks", th.StringType),
            th.Property("conversions", th.StringType),
            th.Property("bounced", th.StringType),
            th.Property("unsubscribed", th.StringType),
            th.Property("cost", th.StringType),
            th.Property("revenue", th.StringType),
            th.Property("profit", th.NumberType),
            th.Property("currency", th.StringType),
            th.Property("active", th.StringType),
            th.Property("link", th.StringType),
        ))),
    ).to_dict()


class BuyersStream(LeadByteStream):
    """Buyers stream."""
    
    name = "buyers"
    path = "/buyers"
    primary_keys = ["company"]  # Assuming company is unique identifier
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("company", th.StringType),
        th.Property("street1", th.StringType),
        th.Property("towncity", th.StringType),
        th.Property("county", th.StringType),
        th.Property("country", th.StringType),
        th.Property("postcode", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("external_ref", th.StringType),
        th.Property("external_ref_2", th.StringType),
        th.Property("status", th.StringType),
        th.Property("credit_amount", th.StringType),
        th.Property("credit_balance", th.StringType),
    ).to_dict()
    
    records_jsonpath = "$.buyers[*]"
