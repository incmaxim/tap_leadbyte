"""Stream type classes for tap-leadbyte."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone
from dateutil import parser

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
        
        # Add date parameters (only if date_preset is not specified)
        if not self.config.get("date_preset"):
            start_date = self.config["start_date"]
            if isinstance(start_date, str):
                start_date = parser.parse(start_date)
            
            params["from"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            if self.config.get("end_date"):
                end_date = self.config["end_date"]
                if isinstance(end_date, str):
                    end_date = parser.parse(end_date)
                params["to"] = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                params["to"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            # Use date preset instead
            params["datePreset"] = self.config["date_preset"]
        
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
        
        # Add debug option
        if self.config.get("debug"):
            params["debug"] = "Yes" if self.config["debug"] else "No"
            
        # Add groupBy option
        if self.config.get("group_by"):
            params["groupBy"] = self.config["group_by"]
            
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
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]
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
        # ✅ Dodati flattened primary key fields u schema:
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


class BulkEmailReportsStream(ReportsStream):
    """Bulk email reports stream."""
    
    name = "bulk_email_reports"
    path = "/reports/bulkemail"
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]
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
        # ✅ Dodati flattened primary key fields:
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


class BulkSmsReportsStream(ReportsStream):
    """Bulk SMS reports stream."""
    
    name = "bulk_sms_reports"
    path = "/reports/bulksms"
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]
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
        # ✅ Dodati flattened primary key fields:
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


class SupplierReportsStream(ReportsStream):
    """Supplier reports stream."""
    
    name = "supplier_reports"
    path = "/reports/supplier"
    primary_keys = ["campaign_id", "supplier_id"]
    replication_key = None
    
    # ✅ DIREKTNO postavljanje schema kao class attribute
    schema = {
        "type": "object",
        "properties": {
            "campaign": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "reference": {"type": ["string", "null"]}
                }
            },
            "supplier": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "sid": {"type": ["string", "null"]}
                }
            },
            "leads": {"type": ["integer", "null"]},
            "valid": {"type": ["integer", "null"]},
            "invalid": {"type": ["integer", "null"]},
            "validCR": {"type": ["number", "null"]},
            "pending": {"type": ["integer", "null"]},
            "rejected": {"type": ["integer", "null"]},
            "payable": {"type": ["integer", "null"]},
            "sold": {"type": ["integer", "null"]},
            "returns": {"type": ["integer", "null"]},
            "payableCR": {"type": ["number", "null"]},
            "payout": {"type": ["number", "null"]},
            "emailCost": {"type": ["number", "null"]},
            "smsCost": {"type": ["number", "null"]},
            "validationCost": {"type": ["number", "null"]},
            "revenue": {"type": ["number", "null"]},
            "profit": {"type": ["number", "null"]},
            "eCPL": {"type": ["number", "null"]},
            "eRPL": {"type": ["number", "null"]},
            "payoutAdjusted": {"type": ["number", "null"]},
            "revenueAdjusted": {"type": ["number", "null"]},
            "profitAdjusted": {"type": ["number", "null"]},
            "eCPLAdjusted": {"type": ["number", "null"]},
            "eRPLAdjusted": {"type": ["number", "null"]},
            "currency": {"type": ["string", "null"]},
            "campaign_id": {
                "type": ["string", "null"],
                "description": "Derived from campaign.id"
            },
            "supplier_id": {
                "type": ["string", "null"],
                "description": "Derived from supplier.id"
            }
        }
    }
    
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add flattened primary key fields."""
        row["campaign_id"] = row["campaign"]["id"]
        if "supplier" in row:
            row["supplier_id"] = row["supplier"]["id"]
        else:
            row["supplier_id"] = "unknown"
        return row
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for supplier reports."""
        params = super().get_url_params(context, next_page_token)
        
        params["showSupplier"] = "Yes"
        params["showCampaign"] = "Yes"
        
        if self.config.get("include_non_supplier_leads"):
            params["includeNonSupplierLeads"] = "Yes"
            
        if self.config.get("lead_type_api") is not None:
            params["leadTypeAPI"] = "Yes" if self.config["lead_type_api"] else "No"
            
        if self.config.get("lead_type_import") is not None:
            params["leadTypeImport"] = "Yes" if self.config["lead_type_import"] else "No"
            
        if self.config.get("show_ssid"):
            params["showSSID"] = "Yes"
            
        return params


class BuyerReportsStream(ReportsStream):
    """Buyer reports stream."""
    
    name = "buyer_reports"
    path = "/reports/buyer"
    primary_keys = ["campaign_id", "buyer_id"]
    replication_key = None
    
    # ✅ DIREKTNO postavljanje schema kao class attribute
    schema = {
        "type": "object",
        "properties": {
            "campaign": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "reference": {"type": ["string", "null"]}
                }
            },
            "supplier": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "sid": {"type": ["string", "null"]}
                }
            },
            "buyer": {
                "type": ["object", "null"],
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "name": {"type": ["string", "null"]},
                    "bid": {"type": ["string", "null"]}
                }
            },
            "posted": {"type": ["integer", "null"]},
            "accepted": {"type": ["integer", "null"]},
            "sold": {"type": ["integer", "null"]},
            "rejected": {"type": ["integer", "null"]},
            "approvedCR": {"type": ["number", "null"]},
            "returned": {"type": ["integer", "null"]},
            "returnedPercent": {"type": ["number", "null"]},
            "revenue": {"type": ["number", "null"]},
            "RPL": {"type": ["number", "null"]},
            "RPS": {"type": ["number", "null"]},
            "currency": {"type": ["string", "null"]},
            "campaign_id": {
                "type": ["string", "null"],
                "description": "Derived from campaign.id"
            },
            "buyer_id": {
                "type": ["string", "null"],
                "description": "Derived from buyer.id"
            }
        }
    }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add flattened primary key fields."""
        row["campaign_id"] = row["campaign"]["id"]
        row["buyer_id"] = row["buyer"]["id"]
        return row

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for buyer reports."""
        params = super().get_url_params(context, next_page_token)
        
        params["showBuyer"] = "Yes"
        params["showSupplier"] = "Yes" 
        params["showCampaign"] = "Yes"
        
        if self.config.get("lead_type_api") is not None:
            params["leadTypeAPI"] = "Yes" if self.config["lead_type_api"] else "No"
            
        if self.config.get("lead_type_import") is not None:
            params["leadTypeImport"] = "Yes" if self.config["lead_type_import"] else "No"
            
        if self.config.get("show_ssid"):
            params["showSSID"] = "Yes"
            
        return params


class CampaignReportsStream(ReportsStream):
    """Campaign reports stream."""
    
    name = "campaign_reports"
    path = "/reports/campaign"
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]
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

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add flattened primary key fields."""
        row["campaign_id"] = row["campaign"]["id"]
        row["responder_id"] = row["responder"]["id"]  
        row["supplier_id"] = row["supplier"]["id"]
        row["push_id"] = row["push"]["id"]
        return row

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for campaign reports."""
        params = super().get_url_params(context, next_page_token)
        
        # Campaign-specific parameters
        if self.config.get("lead_type_api") is not None:
            params["leadTypeAPI"] = "Yes" if self.config["lead_type_api"] else "No"
            
        if self.config.get("lead_type_import") is not None:
            params["leadTypeImported"] = "Yes" if self.config["lead_type_import"] else "No"
            
        if self.config.get("show_campaign"):
            params["showCampaign"] = "Yes"
            
        if self.config.get("show_supplier"):
            params["showSupplier"] = "Yes"
            
        if self.config.get("show_ssid"):
            params["showSSID"] = "Yes"
            
        return params


class LeadActivityReportsStream(ReportsStream):
    """Lead activity reports stream."""
    
    name = "lead_activity_reports"
    path = "/reports/leadactivity"
    primary_keys = ["campaign_id", "responder_id", "supplier_id", "push_id"]
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

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add flattened primary key fields."""
        row["campaign_id"] = row["campaign"]["id"]
        row["responder_id"] = row["responder"]["id"]  
        row["supplier_id"] = row["supplier"]["id"]
        row["push_id"] = row["push"]["id"]
        return row

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for lead activity reports."""
        params = super().get_url_params(context, next_page_token)
        
        # Lead activity specific parameters
        if self.config.get("show_supplier"):
            params["showSupplier"] = "Yes"
            
        if self.config.get("show_ssid"):
            params["showSSID"] = "Yes"
            
        if self.config.get("lead_type_api") is not None:
            params["leadTypeAPI"] = "Yes" if self.config["lead_type_api"] else "No"
            
        if self.config.get("lead_type_import") is not None:
            params["leadTypeImport"] = "Yes" if self.config["lead_type_import"] else "No"
            
        if self.config.get("show_data"):
            params["showData"] = self.config["show_data"]
            
        return params


# Master data streams with status filters
class CampaignsStream(LeadByteStream):
    """Campaigns stream with status filter."""
    
    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("status", th.StringType),
        th.Property("start_date", th.StringType),
        th.Property("end_date", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()
    
    records_jsonpath = "$.campaigns[*]"
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for campaigns."""
        params = super().get_url_params(context, next_page_token)
        
        if self.config.get("campaign_status"):
            params["status"] = self.config["campaign_status"]
            
        if self.config.get("debug"):
            params["debug"] = "Yes" if self.config["debug"] else "No"
            
        return params


class DeliveriesStream(LeadByteStream):
    """Deliveries stream with status filter."""
    
    name = "deliveries"
    path = "/deliveries"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("campaign_id", th.IntegerType),
        th.Property("responder_id", th.IntegerType),
        th.Property("supplier_id", th.IntegerType),
        th.Property("push_id", th.IntegerType),
        th.Property("status", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()
    
    records_jsonpath = "$.deliveries[*]"
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for deliveries."""
        params = super().get_url_params(context, next_page_token)
        
        if self.config.get("delivery_status"):
            params["status"] = self.config["delivery_status"]
            
        if self.config.get("debug"):
            params["debug"] = "Yes" if self.config["debug"] else "No"
            
        return params


class RespondersStream(LeadByteStream):
    """Responders stream."""
    
    name = "responders"
    path = "/responders"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()
    
    records_jsonpath = "$.responders[*]"
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for responders."""
        params = super().get_url_params(context, next_page_token)
        
        if self.config.get("debug"):
            params["debug"] = "Yes" if self.config["debug"] else "No"
            
        return params


class BuyersStream(LeadByteStream):
    """Buyers stream."""
    
    name = "buyers"
    path = "/buyers"
    primary_keys = ["company"]
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
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for buyers."""
        params = super().get_url_params(context, next_page_token)
        
        if self.config.get("buyer_status"):
            params["status"] = self.config["buyer_status"]
            
        if self.config.get("debug"):
            params["debug"] = "Yes" if self.config["debug"] else "No"
            
        return params
