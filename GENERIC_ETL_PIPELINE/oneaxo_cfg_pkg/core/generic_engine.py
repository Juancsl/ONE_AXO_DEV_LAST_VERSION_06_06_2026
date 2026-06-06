from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.dynamic_loader import load_handler_instance
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.enrichment_registry import apply_enrichments
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.exceptions import ValidationRejectFileError
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.model_factory import build_dynamic_model_from_models_config
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.parsers import ensure_list, get_path_value, parse_input_bytes
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transform_rules import apply_transform_rule
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.validation_rules import apply_validations, record_has_errors
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.transformers import transform_output_to_target_format
from airflow.providers.postgres.hooks.postgres import PostgresHook
from GENERIC_ETL_PIPELINE.oneaxo_cfg_pkg.core.api_auth_client import ApiClient
from airflow.models import Variable

POSTGRES_CONN_ID = "gobierno_central_postgres"


def build_record_from_mapping(source_context: dict, mappings: list[dict]) -> dict:
    record = {}

    for mapping in mappings:
        find_config = mapping.get("find_in_list")

        if find_config:
            source_list_path = mapping.get("source_path")
            source_list = get_path_value(source_context, source_list_path) or []

            filter_condition = find_config.get("where", {})
            filter_field = filter_condition.get("field")
            filter_value = filter_condition.get("equals")
            extract_field = find_config.get("extract")

            found_value = None

            if filter_field and extract_field:
                for item in source_list:
                    if isinstance(item, dict) and item.get(filter_field) == filter_value:
                        found_value = item.get(extract_field)
                        break

            raw_value = found_value

        else:
            if "source_paths" in mapping and mapping["source_paths"]:
                raw_value = [get_path_value(source_context, p) for p in mapping["source_paths"]]
            else:
                source_path = mapping.get("source_path")
                raw_value = get_path_value(source_context, source_path) if source_path else None

        value = apply_transform_rule(
            value=raw_value,
            rule=mapping.get("transform_rule"),
            default_value=mapping.get("default_value"),
            params=mapping.get("transform_params", {}),
            source_context=source_context,
        )

        if mapping.get("required", False) and value is None:
            raise ValueError(f"Campo requerido sin valor: {mapping['target_field']}")

        record[mapping["target_field"]] = value

    return record


def build_canonical_payloads_from_bytes(file_bytes: bytes, integration_config: dict):
    payload = parse_input_bytes(
        file_bytes=file_bytes,
        source_format=integration_config["source_format"],
        parser_config=integration_config.get("parser_config", {}),
    )

    if integration_config["engine_mode"] == "custom":
        handler = load_handler_instance(
            integration_config["handler_module_path"],
            integration_config["handler_class"],
        )
        return handler.build_canonical_payloads(
            parsed_payload=payload,
            integration_config=integration_config,
        )

    return _process_source_to_canonical(payload, integration_config)


def build_final_payloads_from_canonical(canonical_payloads, integration_config: dict):
    outputs_config = integration_config.get("outputs_config", {})
    final_outputs_config = outputs_config.get("final_outputs", [{}])[0]

    output_handler_module = final_outputs_config.get("output_handler_module")
    output_handler_class = final_outputs_config.get("output_handler_class")

    if output_handler_module and output_handler_class:
        print(f"Usando handler de salida personalizado: {output_handler_class}")
        handler = load_handler_instance(output_handler_module, output_handler_class)
        return handler.build(canonical_payloads, integration_config)

    print("Usando motor de mapeo 1-a-1 por defecto.")
    return _process_canonical_to_final(canonical_payloads, integration_config)


def _process_source_to_canonical(payload, integration_config: dict):
    integration_id = integration_config["integration_id"]
    entities = integration_config.get("entities_config", [])
    models_config = integration_config.get("models_config", {})
    outputs_config = integration_config.get("outputs_config", {})
    canonical_outputs = outputs_config.get("canonical_outputs", [])
    business_rules = integration_config.get("business_rules_config", {})
    enrichments = business_rules.get("enrichments", [])
    validations = business_rules.get("validations", [])
    validation_behavior = business_rules.get("validation_behavior", {})

    if not entities:
        raise ValueError(f"La integración {integration_id} no tiene entities_config")

    if not canonical_outputs:
        raise ValueError(f"La integración {integration_id} no tiene canonical_outputs")

    root_entities = [
        e for e in entities
        if e.get("entity_kind") == "repeat" and e.get("relative_to_entity_key") is None
    ]

    if len(root_entities) != 1:
        raise ValueError(
            f"{integration_id}: este motor requiere exactamente 1 entidad raíz repeat"
        )

    root_entity = root_entities[0]

    child_entities = [
        e for e in entities
        if e.get("relative_to_entity_key") == root_entity["entity_key"]
    ]

    api_creds = Variable.get("sap_connector_config", deserialize_json=True)

    services = {
        "graphql_cache": {},
        "sap_api_client": ApiClient(**api_creds),
        "postgres_conn": PostgresHook(postgres_conn_id=POSTGRES_CONN_ID),
    }

    model_cache = {}
    collected_issues = []

    def get_model(model_key: str):
        if model_key not in model_cache:
            model_cache[model_key] = build_dynamic_model_from_models_config(
                integration_id=integration_id,
                model_key=model_key,
                models_config=models_config,
            )

        return model_cache[model_key]

    root_source = get_path_value(payload, root_entity.get("source_path", "."))
    root_rows = ensure_list(root_source)

    results = {output["output_key"]: [] for output in canonical_outputs}

    for root_idx, root_raw in enumerate(root_rows, start=1):
        root_obj = get_model(root_entity["model_key"])(**root_raw)

        base_context = {
            root_entity["entity_key"]: root_obj.model_dump(),
        }

        repeat_children = {}

        for entity in child_entities:
            raw_value = get_path_value(root_raw, entity.get("source_path", "."))

            if entity["entity_kind"] == "singleton":
                child_raw = raw_value or {}
                child_obj = get_model(entity["model_key"])(**child_raw)
                base_context[entity["entity_key"]] = child_obj.model_dump()

            elif entity["entity_kind"] == "repeat":
                rows = []

                for child_raw in ensure_list(raw_value):
                    child_obj = get_model(entity["model_key"])(**child_raw)
                    rows.append(child_obj.model_dump())

                repeat_children[entity["entity_key"]] = rows

            else:
                raise ValueError(f"entity_kind no soportado: {entity['entity_kind']}")

        for output in canonical_outputs:
            output_key = output["output_key"]
            output_model = get_model(output["output_model_key"])
            iteration_entity_key = output["iteration_entity_key"]
            mappings = output["mappings"]

            if iteration_entity_key == root_entity["entity_key"]:
                iteration_rows = [base_context[root_entity["entity_key"]]]

            elif iteration_entity_key in repeat_children:
                iteration_rows = repeat_children[iteration_entity_key]

                for key in repeat_children:
                    if key == iteration_entity_key:
                        iteration_rows = repeat_children[key]
                    else:
                        base_context[key] = repeat_children[key]

            elif iteration_entity_key in base_context:
                iteration_rows = [base_context[iteration_entity_key]]

            else:
                iteration_rows = []

            for idx, iteration_row in enumerate(iteration_rows, start=1):
                current_context = {
                    **base_context,
                    "meta": {
                        "idx": idx,
                        "root_idx": root_idx,
                    },
                }

                current_context[iteration_entity_key] = iteration_row

                record = build_record_from_mapping(current_context, mappings)
                record = apply_enrichments(record, enrichments, services=services)
                record = apply_validations(record, validations)

                notif_issues = record.get("_meta", {}).get("notification_issues", [])

                # Excel tiene header en fila 1, por eso el primer registro real es linea2.
                record_identifier = f"linea{root_idx + 1}"

                for issue in notif_issues:
                    collected_issues.append(
                        {
                            "issue_code": issue.get("issue_code"),
                            "severity": issue.get("severity", "error"),
                            "field_name": issue.get("field_name"),
                            "message": issue.get("message"),
                            "record_identifier": record_identifier,
                        }
                    )

                if record_has_errors(record):
                    on_error = validation_behavior.get("on_error", "reject_record")

                    # IMPORTANTE:
                    # Si on_error = reject_file, NO rechazamos inmediatamente.
                    # Seguimos leyendo todo el Excel para recolectar todos los errores.
                    if on_error == "reject_file":
                        continue

                    if on_error == "reject_record":
                        continue

                output_obj = output_model(**{
                    k: v for k, v in record.items() if k != "_meta"
                })

                final_record = output_obj.model_dump()

                if "_meta" in record:
                    final_record["_meta"] = record["_meta"]

                results[output_key].append(final_record)

    # IMPORTANTE:
    # Rechazar hasta el final para enviar todos los errores encontrados,
    # no solo el primer pedimento inválido.
    if collected_issues and validation_behavior.get("on_error") == "reject_file":
        raise ValidationRejectFileError(
            "Archivo rechazado por validacion de negocio:",
            issues=collected_issues,
        )

    if len(results) == 1:
        return next(iter(results.values()))

    return results


def _process_canonical_to_final(canonical_results: dict, integration_config: dict):
    integration_id = integration_config["integration_id"]
    models_config = integration_config.get("models_config", {})
    outputs_config = integration_config.get("outputs_config", {})
    final_outputs = outputs_config.get("final_outputs", [])
    target_format = integration_config["target_format"]

    if final_outputs == [{}]:
        return transform_output_to_target_format(canonical_results, target_format, {})

    model_cache = {}

    def get_model(model_key: str):
        if model_key not in model_cache:
            model_cache[model_key] = build_dynamic_model_from_models_config(
                integration_id=integration_id,
                model_key=model_key,
                models_config=models_config,
            )

        return model_cache[model_key]

    results = {output["output_key"]: [] for output in final_outputs}

    for output in final_outputs:
        output_key = output["output_key"]
        output_model = get_model(output["output_model_key"])
        mappings = output.get("mappings", [])

        source_rows = canonical_results

        for idx, row in enumerate(source_rows, start=1):
            current_context = {
                "canonical_line": row,
                "meta": {
                    "idx": idx,
                },
            }

            record = build_record_from_mapping(current_context, mappings)
            output_obj = output_model(**record)
            final_record = output_obj.model_dump()

            results[output_key].append(final_record)

    results = transform_output_to_target_format(results[output_key], target_format, {})

    return results