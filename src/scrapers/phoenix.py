from __future__ import annotations
import io, logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
import pandas as pd, requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils import normalize_text, utc_now_iso
log = logging.getLogger("cranegenius.phoenix")
FIELD_ALIASES = {"permit_or_record_id":["PermitNumber","Permit Number","PERMITNUMBER"],"record_status":["StatusCurrent","Status","STATUS"],"record_date":["IssuedDate","Issue Date","ISSUEDDATE"],"description_raw":["Description","WorkDescription","ProjectDescription","DESCRIPTION"],"contractor_name_raw":["ContractorName","Contractor Name","Contractor","CONTRACTORNAME"],"project_address":["SiteAddress","Site Address","Address","SITEADDRESS"],"project_city":["SiteCity","City","SITECITY"],"project_state":["SiteState","State","SITESTATE"]}
ARCGIS_URL = "https://services.arcgis.com/ORnXvHHB8P2YFJiP/arcgis/rest/services/Phoenix_Planning_Dev_Permit_Activity/FeatureServer/0/query"
class PhoenixScraper:
    def __init__(self, source_config):
        self.source = source_config; self.source_id = source_config["id"]; self.jurisdiction = source_config["jurisdiction"]
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=10))
    def fetch(self):
        try: return self._fetch_arcgis()
        except Exception as e:
            log.warning("ArcGIS failed (%s), trying PDD fallback", e); return self._fetch_pdd_fallback()
    def _fetch_arcgis(self):
        cutoff = (datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")
        params={"where":f"IssuedDate >= DATE '{cutoff}'","outFields":"*","f":"json","resultRecordCount":2000,"orderByFields":"IssuedDate DESC"}
        log.info("Phoenix: fetching ArcGIS...")
        r=requests.get(ARCGIS_URL,params=params,timeout=60,headers={"User-Agent":"CraneGeniusLeadBot/1.0"}); r.raise_for_status()
        data=r.json()
        if "error" in data: raise ValueError(f"ArcGIS error: {data[chr(39)+'error'+chr(39)]}")
        features=data.get("features",[])
        if not features: raise ValueError("ArcGIS returned 0 features")
        df=pd.DataFrame([f.get("attributes",{}) for f in features]); log.info("Phoenix ArcGIS: %d records",len(df)); return df
    def _fetch_pdd_fallback(self):
        log.info("Phoenix: PDD fallback...")
        start=(datetime.now()-timedelta(days=90)).strftime("%m/%d/%Y"); end=datetime.now().strftime("%m/%d/%Y")
        r=requests.post("https://apps-secure.phoenix.gov/PDD/Search/IssuedPermitDataDownload",data={"PermitType":"Commercial","IssuedDateFrom":start,"IssuedDateTo":end,"submitBtn":"Create File"},headers={"User-Agent":"Mozilla/5.0"},timeout=60); r.raise_for_status()
        df=pd.read_csv(io.StringIO(r.text),low_memory=False,skiprows=1); log.info("Phoenix PDD: %d records",len(df)); return df
    def parse(self, raw_df):
        if raw_df.empty: return []
        cols=list(raw_df.columns); log.info("Phoenix columns: %s",cols[:12])
        col_map={}
        for canonical,aliases in FIELD_ALIASES.items():
            for alias in aliases:
                if alias in cols: col_map[canonical]=alias; break
        rows=[]; captured_at=utc_now_iso()
        for _,row in raw_df.iterrows():
            def get(c,row=row): col=col_map.get(c,""); return normalize_text(row.get(col)) if col else ""
            desc=get("description_raw"); contractor=get("contractor_name_raw")
            if not desc and not contractor: continue
            rows.append({"source_id":self.source_id,"source_type":"permit","jurisdiction":self.jurisdiction,"source_url":"https://apps-secure.phoenix.gov/PDD/Search/IssuedPermit","source_capture_utc":captured_at,"permit_or_record_id":get("permit_or_record_id"),"record_status":get("record_status") or "issued","record_date":get("record_date"),"project_address":get("project_address"),"project_city":get("project_city") or "Phoenix","project_state":get("project_state") or "AZ","contractor_name_raw":contractor,"description_raw":desc})
        log.info("Phoenix: %d usable rows",len(rows)); return rows
    def run(self): return self.parse(self.fetch())
