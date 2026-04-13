from ades.packs.quality_defaults import DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES
from ades.service.models import RegistryValidateGeneralQualityRequest


def test_general_quality_request_uses_pack_default_ambiguity_budget() -> None:
    request = RegistryValidateGeneralQualityRequest(
        bundle_dir="/tmp/general-en-bundle",
        output_dir="/tmp/general-en-quality",
    )

    assert (
        request.max_ambiguous_aliases
        == DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES
    )
