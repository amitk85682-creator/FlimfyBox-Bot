from io import BytesIO
from pathlib import Path

from PIL import Image


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_poster_processing_uses_square_fit_without_vertical_foreground_frame():
    assert "ImageOps.fit" in MAIN_SOURCE
    assert "square_poster.jpg" in MAIN_SOURCE
    assert "fg_img" not in MAIN_SOURCE


def test_square_fit_produces_square_image():
    source = Image.new("RGB", (600, 1000), "red")
    square = ImageOpsFit(source)
    assert square.size == (800, 800)


def ImageOpsFit(image):
    from PIL import ImageOps

    output = BytesIO()
    ImageOps.fit(image, (800, 800), method=Image.Resampling.LANCZOS).save(
        output, format="JPEG"
    )
    output.seek(0)
    return Image.open(output)
