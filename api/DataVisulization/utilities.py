"""Common Methods, Constants, and Utilities for Data Visualization API"""

import io


def get_all_columns(db):
    return db.columns

def getImage(plt):
    bytes_image = io.BytesIO()
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image