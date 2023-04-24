import pystac
import packaging.version

PYSTAC_1_7_0 = packaging.version.parse(pystac.__version__) >= packaging.version.Version(
    "1.7.0"
)
