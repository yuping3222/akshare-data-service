"""AkShare 接口扫描模块"""

from akshare_data.offline.scanner.akshare_scanner import AkShareScanner
from akshare_data.offline.scanner.domain_extractor import DomainExtractor
from akshare_data.offline.scanner.category_inferrer import CategoryInferrer
from akshare_data.offline.scanner.param_inferrer import ParamInferrer

__all__ = [
    "AkShareScanner",
    "DomainExtractor",
    "CategoryInferrer",
    "ParamInferrer",
]
