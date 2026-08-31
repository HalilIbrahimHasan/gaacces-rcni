"""Cleaned XML exporter — serializes enrollee DataFrame back to XML."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class XmlExporter:
    """Export cleaned enrollee records to a consolidated XML file."""

    def export_enrollees(
        self, df: pd.DataFrame, output_stem: str, output_dir: Path
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"cleaned_enrollees_{output_stem}.xml"
        issuer_id = (
            str(df["issuer_id"].iloc[0]) if len(df) > 0 else output_stem.split("_")[0]
        )
        root = ET.Element("enrollments")
        root.set("issuer_id", issuer_id)
        root.set("record_count", str(len(df)))
        if "source_period" in df.columns and len(df) > 0:
            root.set("source_period", str(df["source_period"].iloc[0]))
        for _, row in df.iterrows():
            enrollee_el = ET.SubElement(root, "enrollee")
            for col, val in row.items():
                if pd.isna(val) or val == "":
                    continue
                child = ET.SubElement(enrollee_el, col)
                child.text = str(val)
        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        tree.write(path, encoding="UTF-8", xml_declaration=True)
        logger.info("Exported cleaned XML to %s (%d records)", path, len(df))
        return path
