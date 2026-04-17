from __future__ import annotations

import copy
import json
import subprocess
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
ET.register_namespace("w", W_NS)


def qn(tag: str) -> str:
    return f"{{{W_NS}}}{tag}"


def inspect_quarto_project(paper_dir: Path) -> dict:
    result = subprocess.run(
        ["quarto", "inspect", str(paper_dir)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def build_author_affiliations(metadata: dict) -> tuple[list[dict], list[str]]:
    ordered_affiliations: list[str] = []
    affiliation_indices: dict[str, int] = {}
    processed_authors: list[dict] = []

    for author in metadata.get("author", []):
        author_aff_indices: list[int] = []
        for affiliation in author.get("affiliations", []) or []:
            aff_text = str(affiliation).strip()
            if not aff_text:
                continue
            if aff_text not in affiliation_indices:
                ordered_affiliations.append(aff_text)
                affiliation_indices[aff_text] = len(ordered_affiliations)
            author_aff_indices.append(affiliation_indices[aff_text])
        processed_authors.append(
            {
                "name": str(author.get("name", "")).strip(),
                "indices": author_aff_indices,
            }
        )

    return processed_authors, ordered_affiliations


def paragraph_style(paragraph: ET.Element) -> str:
    style = paragraph.find("./w:pPr/w:pStyle", {"w": W_NS})
    return style.get(qn("val")) if style is not None else ""


def paragraph_text(paragraph: ET.Element) -> str:
    return "".join(node.text or "" for node in paragraph.findall(".//w:t", {"w": W_NS})).strip()


def clear_paragraph_runs(paragraph: ET.Element) -> ET.Element:
    existing_ppr = paragraph.find("./w:pPr", {"w": W_NS})
    ppr = copy.deepcopy(existing_ppr) if existing_ppr is not None else ET.Element(qn("pPr"))
    for child in list(paragraph):
        paragraph.remove(child)
    paragraph.append(ppr)
    return ppr


def add_text_run(paragraph: ET.Element, text: str, superscript: bool = False) -> None:
    run = ET.SubElement(paragraph, qn("r"))
    if superscript:
        rpr = ET.SubElement(run, qn("rPr"))
        vert = ET.SubElement(rpr, qn("vertAlign"))
        vert.set(qn("val"), "superscript")
    text_node = ET.SubElement(run, qn("t"))
    text_node.text = text


def replace_author_paragraph(paragraph: ET.Element, author_name: str, aff_indices: list[int]) -> None:
    clear_paragraph_runs(paragraph)
    add_text_run(paragraph, author_name)
    if aff_indices:
        add_text_run(paragraph, ",".join(str(index) for index in aff_indices), superscript=True)


def make_affiliation_paragraph(aff_index: int, aff_text: str) -> ET.Element:
    paragraph = ET.Element(qn("p"))
    ppr = ET.SubElement(paragraph, qn("pPr"))
    pstyle = ET.SubElement(ppr, qn("pStyle"))
    pstyle.set(qn("val"), "Normal")
    justification = ET.SubElement(ppr, qn("jc"))
    justification.set(qn("val"), "center")
    add_text_run(paragraph, f"{aff_index}. {aff_text}")
    return paragraph


def patch_docx(docx_path: Path, authors: list[dict], affiliations: list[str]) -> bool:
    if not docx_path.exists() or not authors or not affiliations:
        return False

    with zipfile.ZipFile(docx_path, "r") as archive:
        files = {name: archive.read(name) for name in archive.namelist()}

    root = ET.fromstring(files["word/document.xml"])
    body = root.find("./w:body", {"w": W_NS})
    if body is None:
        return False

    body_children = list(body)
    author_positions = [idx for idx, child in enumerate(body_children) if child.tag == qn("p") and paragraph_style(child) == "Author"]
    if not author_positions:
        return False

    first_author_idx = author_positions[0]
    date_idx = next(
        (
            idx
            for idx, child in enumerate(body_children[first_author_idx + len(author_positions) :], start=first_author_idx + len(author_positions))
            if child.tag == qn("p") and paragraph_style(child) == "Date"
        ),
        None,
    )
    if date_idx is None:
        return False

    for paragraph, author in zip((body_children[idx] for idx in author_positions), authors):
        replace_author_paragraph(paragraph, author["name"], author["indices"])

    existing_between = body_children[author_positions[-1] + 1 : date_idx]
    affiliation_texts = {f"{idx}. {text}" for idx, text in enumerate(affiliations, start=1)}
    for node in existing_between:
        if node.tag != qn("p"):
            continue
        if paragraph_text(node) in affiliation_texts:
            body.remove(node)

    insert_at = list(body).index(body_children[author_positions[-1]]) + 1
    for aff_idx, aff_text in enumerate(affiliations, start=1):
        paragraph = make_affiliation_paragraph(aff_idx, aff_text)
        body.insert(insert_at, paragraph)
        insert_at += 1

    files["word/document.xml"] = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    with zipfile.ZipFile(docx_path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)

    return True


def main() -> None:
    paper_dir = Path(__file__).resolve().parent
    project_info = inspect_quarto_project(paper_dir)
    project_config = project_info["config"]
    article_name = project_config["manuscript"]["article"]
    metadata = project_info["fileInformation"][article_name]["metadata"]
    authors, affiliations = build_author_affiliations(metadata)

    output_dir = (paper_dir / project_config["project"]["output-dir"]).resolve()
    output_basename = project_config["format"]["docx"].get("output-file", Path(article_name).stem)
    source_docx_path = paper_dir / f"{output_basename}.docx"
    final_docx_path = output_dir / f"{output_basename}.docx"

    patch_docx(source_docx_path, authors, affiliations)
    patch_docx(final_docx_path, authors, affiliations)


if __name__ == "__main__":
    main()
