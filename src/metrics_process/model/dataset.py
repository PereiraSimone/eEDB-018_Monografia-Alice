# metrics_process/model/dataset.py
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class EndToEnd:
    """Representa uma única regra de negócio dentro de uma validação E2E."""
    active: bool = True
    description: str = ""
    table_source: str = ""
    rule: str = ""
    source_query: str = ""
    expected_result: str = ""

@dataclass
class EndToEndValidations:
    """Representa um conjunto de validações de negócio de ponta a ponta."""
    active: bool = True
    description: str = ""
    end_to_end_validations: List[EndToEnd] = field(default_factory=list)

@dataclass
class Validations:
    """Representa uma única regra de validação de métrica do JSON.
    Todos os campos são opcionais para máxima flexibilidade."""
    active: Optional[bool] = True
    validation_name: Optional[str] = None
    validation_type: Optional[str] = None
    metric: Optional[str] = None
    column: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)
    args: Optional[Dict[str, Any]] = field(default_factory=dict)
    threshold_fail: Optional[float] = None
    threshold_success: Optional[float] = None
    alert_level: Optional[str] = "INFO"

@dataclass
class Dataset:
    name: Optional[str] = None
    source_path: Optional[str] = None
    format: Optional[str] = None
    options: Optional[Dict[str, Any]] = field(default_factory=dict)
    schema: Optional[Dict[str, Any]] = field(default_factory=dict)
    transformations: Optional[List[Dict[str, Any]]] = field(default_factory=list) # <<< O ATRIBUTO QUE FALTAVA
    validations: List[Validations] = field(default_factory=list)
    description: Optional[str] = ""
    end_to_end: List[EndToEndValidations] = field(default_factory=list)
    output_path: Optional[str] = None

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'Dataset':
        return cls(
            name=config.get("dataset"),
            description=config.get("description"),
            source_path=config.get("source_path"),
            format=config.get("format", [""])[0],
            options=config.get("options", {}),
            schema=config.get("schema", {}),
            transformations=config.get("transformations", []), # <<< O MAPEAMENTO QUE FALTAVA
            validations=[Validations(**v) for v in config.get("validations", [])],
            end_to_end=[EndToEndValidations(**e) for e in config.get("end_to_end", [])],
            output_path=config.get("output_path")
        )