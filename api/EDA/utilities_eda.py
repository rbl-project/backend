""" Common methods, constants, and utilities for EDA API """
import io
import base64

def get_all_columns(db):
    return db.columns

def get_image(plt):
    bytes_image = io.BytesIO()
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    image_str = base64.b64encode(bytes_image.read()).decode('utf-8')
    return image_str

# =========CONSTANTS================
ROW_END = "end"