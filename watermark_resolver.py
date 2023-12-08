import cv2
import numpy as np


def remove_watermark(img_name):
    src = cv2.imread('downloaded_images/'+img_name)
    mask = cv2.imread("mask.webp", cv2.IMREAD_GRAYSCALE)

    (h, w, _) = src.shape
    mask = cv2.resize(mask, (w, h))
    dest = cv2.inpaint(src, mask, 3, cv2.INPAINT_NS)

    cv2.imwrite('downloaded_images/'+img_name, dest) 
