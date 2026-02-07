"""JSONパラメータスキーマ定義"""
from dataclasses import dataclass, field
from typing import Literal, Optional
import json


@dataclass
class DateRange:
    start: str  # YYYY-MM-DD
    end: str    # YYYY-MM-DD


@dataclass
class Filter:
    field: str
    op: Literal["==", "!=", ">", "<", ">=", "<=", "contains", "not_contains"]
    value: str


@dataclass
class Visualization:
    type: Literal["table", "line", "bar", "pie"]
    x: Optional[str] = None
    y: Optional[str] = None
    title: Optional[str] = None


@dataclass
class QueryParams:
    """クエリパラメータのスキーマ"""
    schema_version: str
    source: Literal["ga4", "gsc"]
    date_range: DateRange
    dimensions: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    filters: list[Filter] = field(default_factory=list)
    visualization: Optional[Visualization] = None
    
    # GA4 固有
    property_id: Optional[str] = None
    
    # GSC 固有
    site_url: Optional[str] = None
    
    # 共通オプション
    limit: int = 1000

    @classmethod
    def from_json(cls, json_str: str) -> "QueryParams":
        """JSON文字列からQueryParamsを生成"""
        data = json.loads(json_str)
        if data.get("schema_version") != "1.0":
            raise ValueError("schema_version must be '1.0'")
        
        # DateRange
        date_range = DateRange(**data["date_range"])
        
        # Filters
        filters = [Filter(**f) for f in data.get("filters", [])]
        
        # Visualization
        viz = None
        if "visualization" in data:
            viz = Visualization(**data["visualization"])
        
        return cls(
            schema_version=data["schema_version"],
            source=data["source"],
            date_range=date_range,
            dimensions=data.get("dimensions", []),
            metrics=data.get("metrics", []),
            filters=filters,
            visualization=viz,
            property_id=data.get("property_id"),
            site_url=data.get("site_url"),
            limit=data.get("limit", 1000),
        )

    def to_json(self) -> str:
        """JSON文字列に変換"""
        data = {
            "schema_version": self.schema_version,
            "source": self.source,
            "date_range": {"start": self.date_range.start, "end": self.date_range.end},
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "filters": [{"field": f.field, "op": f.op, "value": f.value} for f in self.filters],
            "limit": self.limit,
        }
        if self.visualization:
            data["visualization"] = {
                "type": self.visualization.type,
                "x": self.visualization.x,
                "y": self.visualization.y,
                "title": self.visualization.title,
            }
        if self.property_id:
            data["property_id"] = self.property_id
        if self.site_url:
            data["site_url"] = self.site_url
        return json.dumps(data, indent=2, ensure_ascii=False)


# サンプルJSON
SAMPLE_GA4_JSON = """{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {
    "start": "2026-01-28",
    "end": "2026-02-03"
  },
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"],
  "filters": [
    {"field": "defaultChannelGroup", "op": "==", "value": "Organic Search"}
  ],
  "visualization": {
    "type": "line",
    "x": "date",
    "y": "sessions",
    "title": "Organic Search セッション推移"
  },
  "limit": 1000
}"""

SAMPLE_GSC_JSON = """{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "sc-domain:example.com",
  "date_range": {
    "start": "2026-01-28",
    "end": "2026-02-03"
  },
  "dimensions": ["query"],
  "metrics": ["clicks", "impressions", "ctr", "position"],
  "filters": [],
  "visualization": {
    "type": "bar",
    "x": "query",
    "y": "clicks",
    "title": "クエリ別クリック数"
  },
  "limit": 20
}"""
