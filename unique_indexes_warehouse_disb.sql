CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_transf_reasons" ON warehouse.transfer_reasons USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_opbal" ON warehouse.operators_balance USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_acc_bl" ON warehouse.account_block_list USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_msf" ON warehouse.merchant_settlement_fields USING btree ("dbId","Id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_bd" ON warehouse.bank_details USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_meropid" ON warehouse.merchant_operators USING btree ("dbId","merOprId");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_api_resp" ON warehouse.api_response USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_sfs" ON warehouse.settle_field_slabs USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_tf" ON warehouse.titlefetch_api_response USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_mb" ON warehouse.merchant_balance USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_merh_api_log" ON warehouse.merchant_api_log USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_rev_logs" ON warehouse.reversal_logs USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_reint_disb" ON warehouse.reinitiated_disbursements USING btree ("dbId","id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_Id_disb_sf" ON warehouse.disbursement_settlement_fields USING btree ("dbId","Id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_disbursementid_disb" ON warehouse."disbursements" ("dbId", "disbursementId");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_merchantid_md" ON warehouse.merchant_detail USING btree ("dbId", "merchantID");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_extresp" ON warehouse.external_responses USING btree ("dbId", id);

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_operatorid_op" ON warehouse.operator USING btree ("dbId", "operatorID");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_meroprid_merch_ops" ON warehouse.merchant_operators USING btree ("dbId", "merOprId");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_opsprid_op_sup" ON warehouse.operator_supported USING btree ("dbId", "oprSprId");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_ret_int" ON warehouse.retry_intervals USING btree ("dbId", "id");

CREATE UNIQUE INDEX CONCURRENTLY "uidx_dbid_id_meropr_routes" ON warehouse.meropr_routes USING btree ("dbId", "routeId");