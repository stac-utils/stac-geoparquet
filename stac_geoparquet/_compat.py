import packaging.version
import pystac

PYSTAC_1_7_0 = packaging.version.parse(pystac.__version__) >= packaging.version.Version(
    "1.7.0"
)
