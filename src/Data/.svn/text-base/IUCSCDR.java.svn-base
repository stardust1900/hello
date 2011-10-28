package Data;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import Format.BytesToString;


public class IUCSCDR implements  WriteObject{

	byte[] start_time_s = new byte[4];
	byte[] start_time_ns = new byte[4];
	byte[] end_time_s = new byte[4];
	byte[] end_time_ns = new byte[4];
	byte[] cdr_index = new byte[4];
	byte[] cdr_type = new byte[1];
	byte[] cdr_result = new byte[1];
	byte[] base_cdr_index = new byte[4];
	byte[] base_cdr_type = new byte[1];
	byte[] tmsi = new byte[4];
	byte[] new_tmsi = new byte[4];
	byte[] imsi = new byte[8];
	byte[] imei = new byte[8];
	byte[] calling_number = new byte[8];
	byte[] called_number = new byte[8];
	byte[] third_number = new byte[24];
	byte[] mgw_ip = new byte[4];
	byte[] msc_server_ip = new byte[4];
	byte[] rnc_spc = new byte[4];
	byte[] msc_spc = new byte[4];
	byte[] lac = new byte[2];
	byte[] ci = new byte[2];
	byte[] last_lac = new byte[2];
	byte[] last_ci = new byte[2];
	byte[] cref_cause = new byte[1];
	byte[] cm_rej_cause = new byte[1];
	byte[] lu_rej_cause = new byte[1];
	byte[] assign_failure_cause = new byte[1];
	byte[] rr_cause = new byte[1];
	byte[] cip_rej_cause = new byte[1];
	byte[] disconnect_cause = new byte[1];
	byte[] cc_rel_cause = new byte[1];
	byte[] clear_cause = new byte[1];
	byte[] cp_cause = new byte[1];
	byte[] rp_cause = new byte[1];
	byte[] ho_cause = new byte[24];
	byte[] ho_failure_cause = new byte[24];
	byte[] rab_ass_failure_cause = new byte[2];
	byte[] rab_rel_failure_cause = new byte[2];
	byte[] rab_rel_request_cause = new byte[2];
	byte[] iu_rel_request_cause = new byte[2];
	byte[] iu_rel_command_cause = new byte[2];
	byte[] first_paging_time = new byte[4];
	byte[] second_paging_time = new byte[4];
	byte[] third_paging_time = new byte[4];
	byte[] fourth_paging_time = new byte[4];
	byte[] cc_time = new byte[4];
	byte[] rab_ass_time = new byte[4];
	byte[] rab_ass_complete_time = new byte[4];
	byte[] setup_time = new byte[4];
	byte[] alert_time = new byte[4];
	byte[] connect_time = new byte[4];
	byte[] disconnect_time = new byte[4];
	byte[] iu_release_request_time = new byte[4];
	byte[] iu_release_command_time = new byte[4];
	byte[] rp_data_time = new byte[4];
	byte[] rp_ack_time = new byte[4];
	byte[] auth_request_time = new byte[4];
	byte[] auth_response_time = new byte[4];
	byte[] sec_mode_cmd_time = new byte[4];
	byte[] sec_mode_cmp_time = new byte[4];
	byte[] cm_service_accept_time = new byte[4];
	byte[] call_confirm_preceding_time = new byte[4];
	byte[] connect_ack_time = new byte[4];
	byte[] rab_release_request_time = new byte[4];
	byte[] relocation_request_time = new byte[4];
	byte[] relocation_request_ack_time = new byte[4];
	byte[] relocation_command_time = new byte[4];
	byte[] relocation_complete_time = new byte[4];
	byte[] relocation_detect_time = new byte[4];
	byte[] forward_srns_context_time = new byte[4];
	byte[] smsc = new byte[24];
	byte[] sm_type = new byte[1];
	byte[] sm_data_coding_scheme = new byte[1];
	byte[] sm_length = new byte[2];
	byte[] rp_data_count = new byte[1];
	byte[] handover_count = new byte[1];
	byte[] info_trans_capability = new byte[1];
	byte[] speech_version = new byte[1];
	byte[] failed_handover_count = new byte[1];
	byte[] sub_cdr_index_set = new byte[200];//
	byte[] sub_cdr_starttime_set = new byte[200];//
	byte[] call_stop = new byte[1];
	byte[] interrnc_ho_count = new byte[1];
	byte[] iu_release_cmp_time = new byte[4];
	byte[] max_bitrate = new byte[4];
	byte[] rab_release_dir = new byte[1];
	byte[] bscid = new byte[8];
	byte[] call_stop_msg = new byte[1];
	byte[] call_stop_cause = new byte[1];
	byte[] mscid = new byte[4];
	byte[] last_bscid = new byte[8];
	byte[] last_mscid = new byte[4];
	byte[] dtmf = new byte[1];
	byte[] cid = new byte[4];
	byte[] last_cid = new byte[4];
	byte[] tac = new byte[4];
	byte[] cdr_rel_type = new byte[1];
	
	
	byte[] identity_request_time = new byte[4];
	byte[] identity_response_time= new byte[4];
	byte[] ciph_mode_cmd_time= new byte[4];
	byte[] ciph_mode_cmp_time= new byte[4];
	byte[] tmsi_realloc_cmd_time= new byte[4];
	byte[] tmsi_realloc_cmp_time= new byte[4];
	byte[] cc_release_time = new byte[4];
	byte[] cc_release_cmp_time= new byte[4];
	byte[] clear_cmp_time= new byte[4];
	byte[] sccp_release_time= new byte[4];
	byte[] sccp_release_cmp_time = new byte[4];
	
	public static final int SIZE = 828; 

	public IUCSCDR() {
	}

	public boolean decode(byte[] content) {

		try {
			System.arraycopy(content, 0, start_time_s, 0, 4);
			System.arraycopy(content, 4, start_time_ns, 0, 4);
			System.arraycopy(content, 8, end_time_s, 0, 4);
			System.arraycopy(content, 12, end_time_ns, 0, 4);
			System.arraycopy(content, 16, cdr_index, 0, 4);
			System.arraycopy(content, 20, cdr_type, 0, 1);
			System.arraycopy(content, 21, cdr_result, 0, 1);
			System.arraycopy(content, 22, base_cdr_index, 0, 4);
			System.arraycopy(content, 26, base_cdr_type, 0, 1);
			System.arraycopy(content, 27, tmsi, 0, 4);
			System.arraycopy(content, 31, new_tmsi, 0, 4);
			System.arraycopy(content, 35, imsi, 0, 8);
			System.arraycopy(content, 43, imei, 0, 8);
			System.arraycopy(content, 51, calling_number, 0, 8);
			System.arraycopy(content, 59, called_number, 0, 8);
			System.arraycopy(content, 67, third_number, 0, 24);
			System.arraycopy(content, 91, mgw_ip, 0, 4);
			System.arraycopy(content, 95, msc_server_ip, 0, 4);
			System.arraycopy(content, 99, rnc_spc, 0, 4);
			System.arraycopy(content, 103, msc_spc, 0, 4);
			System.arraycopy(content, 107, lac, 0, 2);
			System.arraycopy(content, 109, ci, 0, 2);
			System.arraycopy(content, 111, last_lac, 0, 2);
			System.arraycopy(content, 113, last_ci, 0, 2);
			System.arraycopy(content, 115, cref_cause, 0, 1);
			System.arraycopy(content, 116, cm_rej_cause, 0, 1);
			System.arraycopy(content, 117, lu_rej_cause, 0, 1);
			System.arraycopy(content, 118, assign_failure_cause, 0, 1);
			System.arraycopy(content, 119, rr_cause, 0, 1);
			System.arraycopy(content, 120, cip_rej_cause, 0, 1);
			System.arraycopy(content, 121, disconnect_cause, 0, 1);
			System.arraycopy(content, 122, cc_rel_cause, 0, 1);
			System.arraycopy(content, 123, clear_cause, 0, 1);
			System.arraycopy(content, 124, cp_cause, 0, 1);
			System.arraycopy(content, 125, rp_cause, 0, 1);
			System.arraycopy(content, 126, ho_cause, 0, 24);
			System.arraycopy(content, 150, ho_failure_cause, 0, 24);
			System.arraycopy(content, 174, rab_ass_failure_cause, 0, 2);
			System.arraycopy(content, 176, rab_rel_failure_cause, 0, 2);
			System.arraycopy(content, 178, rab_rel_request_cause, 0, 2);
			System.arraycopy(content, 180, iu_rel_request_cause, 0, 2);
			System.arraycopy(content, 182, iu_rel_command_cause, 0, 2);
			System.arraycopy(content, 184, first_paging_time, 0, 4);
			System.arraycopy(content, 188, second_paging_time, 0, 4);
			System.arraycopy(content, 192, third_paging_time, 0, 4);
			System.arraycopy(content, 196, fourth_paging_time, 0, 4);
			System.arraycopy(content, 200, cc_time, 0, 4);
			System.arraycopy(content, 204, rab_ass_time, 0, 4);
			System.arraycopy(content, 208, rab_ass_complete_time, 0, 4);
			System.arraycopy(content, 212, setup_time, 0, 4);
			System.arraycopy(content, 216, alert_time, 0, 4);
			System.arraycopy(content, 220, connect_time, 0, 4);
			System.arraycopy(content, 224, disconnect_time, 0, 4);
			System.arraycopy(content, 228, iu_release_request_time, 0, 4);
			System.arraycopy(content, 232, iu_release_command_time, 0, 4);
			System.arraycopy(content, 236, rp_data_time, 0, 4);
			System.arraycopy(content, 240, rp_ack_time, 0, 4);
			System.arraycopy(content, 244, auth_request_time, 0, 4);
			System.arraycopy(content, 248, auth_response_time, 0, 4);
			System.arraycopy(content, 252, sec_mode_cmd_time, 0, 4);
			System.arraycopy(content, 256, sec_mode_cmp_time, 0, 4);
			System.arraycopy(content, 260, cm_service_accept_time, 0, 4);
			System.arraycopy(content, 264, call_confirm_preceding_time, 0, 4);
			System.arraycopy(content, 268, connect_ack_time, 0, 4);
			System.arraycopy(content, 272, rab_release_request_time, 0, 4);
			System.arraycopy(content, 276, relocation_request_time, 0, 4);
			System.arraycopy(content, 280, relocation_request_ack_time, 0, 4);
			System.arraycopy(content, 284, relocation_command_time, 0, 4);
			System.arraycopy(content, 288, relocation_complete_time, 0, 4);
			System.arraycopy(content, 292, relocation_detect_time, 0, 4);
			System.arraycopy(content, 296, forward_srns_context_time, 0, 4);
			System.arraycopy(content, 300, smsc, 0, 24);
			System.arraycopy(content, 324, sm_type, 0, 1);
			System.arraycopy(content, 325, sm_data_coding_scheme, 0, 1);
			System.arraycopy(content, 326, sm_length, 0, 2);
			System.arraycopy(content, 328, rp_data_count, 0, 1);
			System.arraycopy(content, 329, handover_count, 0, 1);
			System.arraycopy(content, 330, info_trans_capability, 0, 1);
			System.arraycopy(content, 331, speech_version, 0, 1);
			System.arraycopy(content, 332, failed_handover_count, 0, 1);
			System.arraycopy(content, 333, sub_cdr_index_set, 0, 200);
			System.arraycopy(content, 533, sub_cdr_starttime_set, 0, 200);
			System.arraycopy(content, 733, call_stop, 0, 1);
			System.arraycopy(content, 734, interrnc_ho_count, 0, 1);
			System.arraycopy(content, 735, iu_release_cmp_time, 0, 4);
			System.arraycopy(content, 739, max_bitrate, 0, 4);
			System.arraycopy(content, 743, rab_release_dir, 0, 1);
			System.arraycopy(content, 744, bscid, 0, 8);
			System.arraycopy(content, 752, call_stop_msg, 0, 1);
			System.arraycopy(content, 753, call_stop_cause, 0, 1);
			System.arraycopy(content, 754, mscid, 0, 4);
			System.arraycopy(content, 758, last_bscid, 0, 8);
			System.arraycopy(content, 766, last_mscid, 0, 4);
			System.arraycopy(content, 770, dtmf, 0, 1);
			System.arraycopy(content, 771, cid, 0, 4);
			System.arraycopy(content, 775, last_cid, 0, 4);
			System.arraycopy(content, 779, tac, 0, 4);
			System.arraycopy(content, 783, cdr_rel_type, 0, 1);
			
			
			System.arraycopy(content, 784, identity_request_time , 0, 4);
			System.arraycopy(content, 788, identity_response_time , 0, 4);
			System.arraycopy(content, 792, ciph_mode_cmd_time , 0, 4);
			System.arraycopy(content, 796, ciph_mode_cmp_time , 0, 4);
			System.arraycopy(content, 800, tmsi_realloc_cmd_time , 0, 4);
			System.arraycopy(content, 804, tmsi_realloc_cmp_time , 0, 4);
			System.arraycopy(content, 808, cc_release_time  , 0, 4);
			
			System.arraycopy(content, 812, cc_release_cmp_time  , 0, 4);
			System.arraycopy(content, 816, clear_cmp_time  , 0, 4);
			System.arraycopy(content, 820, sccp_release_time  , 0, 4);
			System.arraycopy(content, 824, sccp_release_cmp_time   , 0, 4);
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public String getStart_time_s() {
		return BytesToString.Bytes4ToString(start_time_s);
	}

	public String getStart_time_ns() {
		return BytesToString.Bytes4ToString(start_time_ns);
	}

	public String getEnd_time_s() {
		return BytesToString.Bytes4ToString(end_time_s);
	}

	public long getEnd_time_s_long() 
	{
		return Long.parseLong(getEnd_time_s());
	}
	public String getEnd_time_ns() {
		return BytesToString.Bytes4ToString(end_time_ns);
	}

	public String getCdr_index() {
		return BytesToString.Bytes4ToString(cdr_index);
	}

	public String getCdr_type() {
		return BytesToString.Byte1ToString(cdr_type);
	}

	public String getCdr_result() {
		return BytesToString.Byte1ToString(cdr_result);
	}

	public String getBase_cdr_index() {
		return BytesToString.Bytes4ToString(base_cdr_index);
	}

	public String getBase_cdr_type() {
		return BytesToString.Byte1ToString(base_cdr_type);
	}

	public String getTmsi() {
		return BytesToString.Bytes4ToString(tmsi);
	}

	public String getNew_tmsi() {
		return BytesToString.Bytes4ToString(new_tmsi);
	}

	public String getImsi() {
		return BytesToString.Bytes8toString(imsi);
	}

	public String getImei() {
		return BytesToString.Bytes8toString(imei);
	}

	public String getCalling_number() {
		return BytesToString.Bytes8toString(calling_number);
	}

	public String getCalled_number() {
		return BytesToString.Bytes8toString(called_number);
	}

	public String getThird_number() {
		return BytesToString.CharstoString(third_number);// chars
	}

	public String getMgw_ip() {
		return BytesToString.Bytes4ToString(mgw_ip);
	}

	public String getMsc_server_ip() {
		return BytesToString.Bytes4ToString(msc_server_ip);
	}

	public String getRnc_spc() {
		return BytesToString.Bytes4ToString(rnc_spc);
	}

	public String getMsc_spc() {
		return BytesToString.Bytes4ToString(msc_spc);
	}

	public String getLac() {
		return BytesToString.Bytes2ToString(lac);
	}

	public String getCi() {
		return BytesToString.Bytes2ToString(ci);
	}

	public String getLast_lac() {
		return BytesToString.Bytes2ToString(last_lac);
	}

	public String getLast_ci() {
		return BytesToString.Bytes2ToString(last_ci);
	}

	public String getCref_cause() {
		return BytesToString.Byte1ToString(cref_cause);
	}

	public String getCm_rej_cause() {
		return BytesToString.Byte1ToString(cm_rej_cause);
	}

	public String getLu_rej_cause() {
		return BytesToString.Byte1ToString(lu_rej_cause);
	}

	public String getAssign_failure_cause() {
		return BytesToString.Byte1ToString(assign_failure_cause);
	}

	public String getRr_cause() {
		return BytesToString.Byte1ToString(rr_cause);
	}

	public String getCip_rej_cause() {
		return BytesToString.Byte1ToString(cip_rej_cause);
	}

	public String getDisconnect_cause() {
		return BytesToString.Byte1ToString(disconnect_cause);
	}

	public String getCc_rel_cause() {
		return BytesToString.Byte1ToString(cc_rel_cause);
	}

	public String getClear_cause() {
		return BytesToString.Byte1ToString(clear_cause);
	}

	public String getCp_cause() {
		return BytesToString.Byte1ToString(cp_cause);
	}

	public String getRp_cause() {
		return BytesToString.Byte1ToString(rp_cause);
	}

	public String getHo_cause() {
		return BytesToString.CharstoString(ho_cause);// chars
	}

	public String getHo_failure_cause() {
		return BytesToString.CharstoString(ho_failure_cause);// chars
	}

	public String getRab_ass_failure_cause() {
		return BytesToString.Bytes2ToString(rab_ass_failure_cause);
	}

	public String getRab_rel_failure_cause() {
		return BytesToString.Bytes2ToString(rab_rel_failure_cause);
	}

	public String getRab_rel_request_cause() {
		return BytesToString.Bytes2ToString(rab_rel_request_cause);
	}

	public String getIu_rel_request_cause() {
		return BytesToString.Bytes2ToString(iu_rel_request_cause);
	}

	public String getIu_rel_command_cause() {
		return BytesToString.Bytes2ToString(iu_rel_command_cause);
	}

	public String getFirst_paging_time() {
		return BytesToString.Bytes4ToString(first_paging_time);
	}

	public String getSecond_paging_time() {
		return BytesToString.Bytes4ToString(second_paging_time);
	}

	public String getThird_paging_time() {
		return BytesToString.Bytes4ToString(third_paging_time);
	}

	public String getFourth_paging_time() {
		return BytesToString.Bytes4ToString(fourth_paging_time);
	}

	public String getCc_time() {
		return BytesToString.Bytes4ToString(cc_time);
	}

	public String getRab_ass_time() {
		return BytesToString.Bytes4ToString(rab_ass_time);
	}

	public String getRab_ass_complete_time() {
		return BytesToString.Bytes4ToString(rab_ass_complete_time);
	}

	public String getSetup_time() {
		return BytesToString.Bytes4ToString(setup_time);
	}

	public String getAlert_time() {
		return BytesToString.Bytes4ToString(alert_time);
	}

	public String getConnect_time() {
		return BytesToString.Bytes4ToString(connect_time);
	}

	public String getDisconnect_time() {
		return BytesToString.Bytes4ToString(disconnect_time);
	}

	public String getIu_release_request_time() {
		return BytesToString.Bytes4ToString(iu_release_request_time);
	}

	public String getIu_release_command_time() {
		return BytesToString.Bytes4ToString(iu_release_command_time);
	}

	public String getRp_data_time() {
		return BytesToString.Bytes4ToString(rp_data_time);
	}

	public String getRp_ack_time() {
		return BytesToString.Bytes4ToString(rp_ack_time);
	}

	public String getAuth_request_time() {
		return BytesToString.Bytes4ToString(auth_request_time);
	}

	public String getAuth_response_time() {
		return BytesToString.Bytes4ToString(auth_response_time);
	}

	public String getSec_mode_cmd_time() {
		return BytesToString.Bytes4ToString(sec_mode_cmd_time);
	}

	public String getSec_mode_cmp_time() {
		return BytesToString.Bytes4ToString(sec_mode_cmp_time);
	}

	public String getCm_service_accept_time() {
		return BytesToString.Bytes4ToString(cm_service_accept_time);
	}

	public String getCall_confirm_preceding_time() {
		return BytesToString.Bytes4ToString(call_confirm_preceding_time);
	}

	public String getConnect_ack_time() {
		return BytesToString.Bytes4ToString(connect_ack_time);
	}

	public String getRab_release_request_time() {
		return BytesToString.Bytes4ToString(rab_release_request_time);
	}

	public String getRelocation_request_time() {
		return BytesToString.Bytes4ToString(relocation_request_time);
	}

	public String getRelocation_request_ack_time() {
		return BytesToString.Bytes4ToString(relocation_request_ack_time);
	}

	public String getRelocation_command_time() {
		return BytesToString.Bytes4ToString(relocation_command_time);
	}

	public String getRelocation_complete_time() {
		return BytesToString.Bytes4ToString(relocation_complete_time);
	}

	public String getRelocation_detect_time() {
		return BytesToString.Bytes4ToString(relocation_detect_time);
	}

	public String getForward_srns_context_time() {
		return BytesToString.Bytes4ToString(forward_srns_context_time);
	}

	public String getSmsc() {
		return BytesToString.CharstoString(smsc);
	}

	public String getSm_type() {
		return BytesToString.Byte1ToString(sm_type);
	}

	public String getSm_data_coding_scheme() {
		return BytesToString.Byte1ToString(sm_data_coding_scheme);
	}

	public String getSm_length() {
		return BytesToString.Bytes2ToString(sm_length);
	}

	public String getRp_data_count() {
		return BytesToString.Byte1ToString(rp_data_count);
	}

	public String getHandover_count() {
		return BytesToString.Byte1ToString(handover_count);
	}

	public String getInfo_trans_capability() {
		return BytesToString.Byte1ToString(info_trans_capability);
	}

	public String getSpeech_version() {
		return BytesToString.Byte1ToString(speech_version);
	}

	public String getFailed_handover_count() {
		return BytesToString.Byte1ToString(failed_handover_count);
	}

	public String getSub_cdr_index_set() {
		return BytesToString.CharstoString(sub_cdr_index_set);// chars
	}

	public String getSub_cdr_starttime_set() {
		return BytesToString.CharstoString(sub_cdr_starttime_set);// chars
	}

	public String getCall_stop() {
		return BytesToString.Byte1ToString(call_stop);
	}

	public String getInterrnc_ho_count() {
		return BytesToString.Byte1ToString(interrnc_ho_count);
	}

	public String getIu_release_cmp_time() {
		return BytesToString.Bytes4ToString(iu_release_cmp_time);
	}

	public String getMax_bitrate() {
		return BytesToString.Bytes4ToString(max_bitrate);
	}

	public String getRab_release_dir() {
		return BytesToString.Byte1ToString(rab_release_dir);
	}

	public String getBscid() {
		return BytesToString.Bytes8toString(bscid);
	}

	public String getCall_stop_msg() {
		return BytesToString.Byte1ToString(call_stop_msg);
	}

	public String getCall_stop_cause() {
		return BytesToString.Byte1ToString(call_stop_cause);
	}

	public String getMscid() {
		return BytesToString.Bytes4ToString(mscid);
	}

	public String getLast_bscid() {
		return BytesToString.Bytes8toString(last_bscid);
	}

	public String getLast_mscid() {
		return BytesToString.Bytes4ToString(last_mscid);
	}

	public String getDtmf() {
		return BytesToString.Byte1ToString(dtmf);
	}

	public String getCid() {
		return BytesToString.Bytes4ToString(cid);
	}

	public String getLast_cid() {
		return BytesToString.Bytes4ToString(last_cid);
	}

	public String getTac() {
		return BytesToString.Bytes4ToString(tac);
	}

	public String getCdr_rel_type() {
		return BytesToString.Byte1ToString(cdr_rel_type);
	}
	
	public String getIdentity_request_time() {
		return BytesToString.Bytes4ToString(identity_request_time);
	}
	public String getIdentity_response_time() {
		return BytesToString.Bytes4ToString(identity_response_time);
	}
	public String getCiph_mode_cmd_time() {
		return BytesToString.Bytes4ToString(ciph_mode_cmd_time);
	}
	public String getCiph_mode_cmp_time() {
		return BytesToString.Bytes4ToString(ciph_mode_cmp_time);
	}
	public String getTmsi_realloc_cmd_time() {
		return BytesToString.Bytes4ToString(tmsi_realloc_cmd_time);
	}
	public String getTmsi_realloc_cmp_time() {
		return BytesToString.Bytes4ToString(tmsi_realloc_cmp_time);
	}
	public String getCc_release_time () {
		return BytesToString.Bytes4ToString(cc_release_time );
	}
	public String getCc_release_cmp_time() {
		return BytesToString.Bytes4ToString(cc_release_cmp_time);
	}
	public String getClear_cmp_time() {
		return BytesToString.Bytes4ToString(clear_cmp_time);
	}
	public String getSccp_release_time() {
		return BytesToString.Bytes4ToString(sccp_release_time);
	}
	public String getSccp_release_cmp_time () {
		return BytesToString.Bytes4ToString(sccp_release_cmp_time );
	}
	
	public boolean  ReadIucscdr(FSDataInputStream hdfsStream) {

		try {
			byte[] content = new byte[SIZE];
			hdfsStream.read(content, 0, SIZE);
			System.arraycopy(content, 0, start_time_s, 0, 4);
			System.arraycopy(content, 4, start_time_ns, 0, 4);
			System.arraycopy(content, 8, end_time_s, 0, 4);
			System.arraycopy(content, 12, end_time_ns, 0, 4);
			System.arraycopy(content, 16, cdr_index, 0, 4);
			System.arraycopy(content, 20, cdr_type, 0, 1);
			System.arraycopy(content, 21, cdr_result, 0, 1);
			System.arraycopy(content, 22, base_cdr_index, 0, 4);
			System.arraycopy(content, 26, base_cdr_type, 0, 1);
			System.arraycopy(content, 27, tmsi, 0, 4);
			System.arraycopy(content, 31, new_tmsi, 0, 4);
			System.arraycopy(content, 35, imsi, 0, 8);
			System.arraycopy(content, 43, imei, 0, 8);
			System.arraycopy(content, 51, calling_number, 0, 8);
			System.arraycopy(content, 59, called_number, 0, 8);
			System.arraycopy(content, 67, third_number, 0, 24);
			System.arraycopy(content, 91, mgw_ip, 0, 4);
			System.arraycopy(content, 95, msc_server_ip, 0, 4);
			System.arraycopy(content, 99, rnc_spc, 0, 4);
			System.arraycopy(content, 103, msc_spc, 0, 4);
			System.arraycopy(content, 107, lac, 0, 2);
			System.arraycopy(content, 109, ci, 0, 2);
			System.arraycopy(content, 111, last_lac, 0, 2);
			System.arraycopy(content, 113, last_ci, 0, 2);
			System.arraycopy(content, 115, cref_cause, 0, 1);
			System.arraycopy(content, 116, cm_rej_cause, 0, 1);
			System.arraycopy(content, 117, lu_rej_cause, 0, 1);
			System.arraycopy(content, 118, assign_failure_cause, 0, 1);
			System.arraycopy(content, 119, rr_cause, 0, 1);
			System.arraycopy(content, 120, cip_rej_cause, 0, 1);
			System.arraycopy(content, 121, disconnect_cause, 0, 1);
			System.arraycopy(content, 122, cc_rel_cause, 0, 1);
			System.arraycopy(content, 123, clear_cause, 0, 1);
			System.arraycopy(content, 124, cp_cause, 0, 1);
			System.arraycopy(content, 125, rp_cause, 0, 1);
			System.arraycopy(content, 126, ho_cause, 0, 24);
			System.arraycopy(content, 150, ho_failure_cause, 0, 24);
			System.arraycopy(content, 174, rab_ass_failure_cause, 0, 2);
			System.arraycopy(content, 176, rab_rel_failure_cause, 0, 2);
			System.arraycopy(content, 178, rab_rel_request_cause, 0, 2);
			System.arraycopy(content, 180, iu_rel_request_cause, 0, 2);
			System.arraycopy(content, 182, iu_rel_command_cause, 0, 2);
			System.arraycopy(content, 184, first_paging_time, 0, 4);
			System.arraycopy(content, 188, second_paging_time, 0, 4);
			System.arraycopy(content, 192, third_paging_time, 0, 4);
			System.arraycopy(content, 196, fourth_paging_time, 0, 4);
			System.arraycopy(content, 200, cc_time, 0, 4);
			System.arraycopy(content, 204, rab_ass_time, 0, 4);
			System.arraycopy(content, 208, rab_ass_complete_time, 0, 4);
			System.arraycopy(content, 212, setup_time, 0, 4);
			System.arraycopy(content, 216, alert_time, 0, 4);
			System.arraycopy(content, 220, connect_time, 0, 4);
			System.arraycopy(content, 224, disconnect_time, 0, 4);
			System.arraycopy(content, 228, iu_release_request_time, 0, 4);
			System.arraycopy(content, 232, iu_release_command_time, 0, 4);
			System.arraycopy(content, 236, rp_data_time, 0, 4);
			System.arraycopy(content, 240, rp_ack_time, 0, 4);
			System.arraycopy(content, 244, auth_request_time, 0, 4);
			System.arraycopy(content, 248, auth_response_time, 0, 4);
			System.arraycopy(content, 252, sec_mode_cmd_time, 0, 4);
			System.arraycopy(content, 256, sec_mode_cmp_time, 0, 4);
			System.arraycopy(content, 260, cm_service_accept_time, 0, 4);
			System.arraycopy(content, 264, call_confirm_preceding_time, 0, 4);
			System.arraycopy(content, 268, connect_ack_time, 0, 4);
			System.arraycopy(content, 272, rab_release_request_time, 0, 4);
			System.arraycopy(content, 276, relocation_request_time, 0, 4);
			System.arraycopy(content, 280, relocation_request_ack_time, 0, 4);
			System.arraycopy(content, 284, relocation_command_time, 0, 4);
			System.arraycopy(content, 288, relocation_complete_time, 0, 4);
			System.arraycopy(content, 292, relocation_detect_time, 0, 4);
			System.arraycopy(content, 296, forward_srns_context_time, 0, 4);
			System.arraycopy(content, 300, smsc, 0, 24);
			System.arraycopy(content, 324, sm_type, 0, 1);
			System.arraycopy(content, 325, sm_data_coding_scheme, 0, 1);
			System.arraycopy(content, 326, sm_length, 0, 2);
			System.arraycopy(content, 328, rp_data_count, 0, 1);
			System.arraycopy(content, 329, handover_count, 0, 1);
			System.arraycopy(content, 330, info_trans_capability, 0, 1);
			System.arraycopy(content, 331, speech_version, 0, 1);
			System.arraycopy(content, 332, failed_handover_count, 0, 1);
			System.arraycopy(content, 333, sub_cdr_index_set, 0, 200);
			System.arraycopy(content, 533, sub_cdr_starttime_set, 0, 200);
			System.arraycopy(content, 733, call_stop, 0, 1);
			System.arraycopy(content, 734, interrnc_ho_count, 0, 1);
			System.arraycopy(content, 735, iu_release_cmp_time, 0, 4);
			System.arraycopy(content, 739, max_bitrate, 0, 4);
			System.arraycopy(content, 743, rab_release_dir, 0, 1);
			System.arraycopy(content, 744, bscid, 0, 8);
			System.arraycopy(content, 752, call_stop_msg, 0, 1);
			System.arraycopy(content, 753, call_stop_cause, 0, 1);
			System.arraycopy(content, 754, mscid, 0, 4);
			System.arraycopy(content, 758, last_bscid, 0, 8);
			System.arraycopy(content, 766, last_mscid, 0, 4);
			System.arraycopy(content, 770, dtmf, 0, 1);
			System.arraycopy(content, 771, cid, 0, 4);
			System.arraycopy(content, 775, last_cid, 0, 4);
			System.arraycopy(content, 779, tac, 0, 4);
			System.arraycopy(content, 783, cdr_rel_type, 0, 1);
			
			System.arraycopy(content, 784, identity_request_time , 0, 4);
			System.arraycopy(content, 788, identity_response_time , 0, 4);
			System.arraycopy(content, 792, ciph_mode_cmd_time , 0, 4);
			System.arraycopy(content, 796, ciph_mode_cmp_time , 0, 4);
			System.arraycopy(content, 800, tmsi_realloc_cmd_time , 0, 4);
			System.arraycopy(content, 804, tmsi_realloc_cmp_time , 0, 4);
			System.arraycopy(content, 808, cc_release_time  , 0, 4);
			
			System.arraycopy(content, 812, cc_release_cmp_time  , 0, 4);
			System.arraycopy(content, 816, clear_cmp_time  , 0, 4);
			System.arraycopy(content, 820, sccp_release_time  , 0, 4);
			System.arraycopy(content, 824, sccp_release_cmp_time   , 0, 4);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
		
		public void Write(FileOutputStream out) {
			if (out != null) {
				try {

					out.write(start_time_s);
					out.write(start_time_ns);
					out.write(end_time_s);
					out.write(end_time_ns);
					out.write(cdr_index);
					out.write(cdr_type);
					out.write(cdr_result);
					out.write(base_cdr_index);
					out.write(base_cdr_type);
					out.write(tmsi);
					out.write(new_tmsi);
					out.write(imsi);
					out.write(imei);
					out.write(calling_number);
					out.write(called_number);
					out.write(third_number);
					out.write(mgw_ip);
					out.write(msc_server_ip);
					out.write(rnc_spc);
					out.write(msc_spc);
					out.write(lac);
					out.write(ci);
					out.write(last_lac);
					out.write(last_ci);
					out.write(cref_cause);
					out.write(cm_rej_cause);
					out.write(lu_rej_cause);
					out.write(assign_failure_cause);
					out.write(rr_cause);
					out.write(cip_rej_cause);
					out.write(disconnect_cause);
					out.write(cc_rel_cause);
					out.write(clear_cause);
					out.write(cp_cause);
					out.write(rp_cause);
					out.write(ho_cause);
					out.write(ho_failure_cause);
					out.write(rab_ass_failure_cause);
					out.write(rab_rel_failure_cause);
					out.write(rab_rel_request_cause);
					out.write(iu_rel_request_cause);
					out.write(iu_rel_command_cause);
					out.write(first_paging_time);
					out.write(second_paging_time);
					out.write(third_paging_time);
					out.write(fourth_paging_time);
					out.write(cc_time);
					out.write(rab_ass_time);
					out.write(rab_ass_complete_time);
					out.write(setup_time);
					out.write(alert_time);
					out.write(connect_time);
					out.write(disconnect_time);
					out.write(iu_release_request_time);
					out.write(iu_release_command_time);
					out.write(rp_data_time);
					out.write(rp_ack_time);
					out.write(auth_request_time);
					out.write(auth_response_time);
					out.write(sec_mode_cmd_time);
					out.write(sec_mode_cmp_time);
					out.write(cm_service_accept_time);
					out.write(call_confirm_preceding_time);
					out.write(connect_ack_time);
					out.write(rab_release_request_time);
					out.write(relocation_request_time);
					out.write(relocation_request_ack_time);
					out.write(relocation_command_time);
					out.write(relocation_complete_time);
					out.write(relocation_detect_time);
					out.write(forward_srns_context_time);
					out.write(smsc);
					out.write(sm_type);
					out.write(sm_data_coding_scheme);
					out.write(sm_length);
					out.write(rp_data_count);
					out.write(handover_count);
					out.write(info_trans_capability);
					out.write(speech_version);
					out.write(failed_handover_count);
					out.write(sub_cdr_index_set);
					out.write(sub_cdr_starttime_set);
					out.write(call_stop);
					out.write(interrnc_ho_count);
					out.write(iu_release_cmp_time);
					out.write(max_bitrate);
					out.write(rab_release_dir);
					out.write(bscid);
					out.write(call_stop_msg);
					out.write(call_stop_cause);
					out.write(mscid);
					out.write(last_bscid);
					out.write(last_mscid);
					out.write(dtmf);
					out.write(cid);
					out.write(last_cid);
					out.write(tac);
					out.write(cdr_rel_type);
					
					out.write(identity_request_time);
					out.write(identity_response_time);
					out.write(ciph_mode_cmd_time);
					out.write(ciph_mode_cmp_time);
					out.write(tmsi_realloc_cmd_time);
					out.write(tmsi_realloc_cmp_time);
					out.write(cc_release_time);
					out.write(cc_release_cmp_time);
					out.write(clear_cmp_time);
					out.write(sccp_release_time);
					out.write(sccp_release_cmp_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		public  boolean ReadCDRFromHDFS(FSDataInputStream hdfsStream) {

			try {
				byte[] content = new byte[SIZE];
				hdfsStream.read(content, 0, SIZE);
				System.arraycopy(content, 0, start_time_s, 0, 4);
				System.arraycopy(content, 4, start_time_ns, 0, 4);
				System.arraycopy(content, 8, end_time_s, 0, 4);
				System.arraycopy(content, 12, end_time_ns, 0, 4);
				System.arraycopy(content, 16, cdr_index, 0, 4);
				System.arraycopy(content, 20, cdr_type, 0, 1);
				System.arraycopy(content, 21, cdr_result, 0, 1);
				System.arraycopy(content, 22, base_cdr_index, 0, 4);
				System.arraycopy(content, 26, base_cdr_type, 0, 1);
				System.arraycopy(content, 27, tmsi, 0, 4);
				System.arraycopy(content, 31, new_tmsi, 0, 4);
				System.arraycopy(content, 35, imsi, 0, 8);
				System.arraycopy(content, 43, imei, 0, 8);
				System.arraycopy(content, 51, calling_number, 0, 8);
				System.arraycopy(content, 59, called_number, 0, 8);
				System.arraycopy(content, 67, third_number, 0, 24);
				System.arraycopy(content, 91, mgw_ip, 0, 4);
				System.arraycopy(content, 95, msc_server_ip, 0, 4);
				System.arraycopy(content, 99, rnc_spc, 0, 4);
				System.arraycopy(content, 103, msc_spc, 0, 4);
				System.arraycopy(content, 107, lac, 0, 2);
				System.arraycopy(content, 109, ci, 0, 2);
				System.arraycopy(content, 111, last_lac, 0, 2);
				System.arraycopy(content, 113, last_ci, 0, 2);
				System.arraycopy(content, 115, cref_cause, 0, 1);
				System.arraycopy(content, 116, cm_rej_cause, 0, 1);
				System.arraycopy(content, 117, lu_rej_cause, 0, 1);
				System.arraycopy(content, 118, assign_failure_cause, 0, 1);
				System.arraycopy(content, 119, rr_cause, 0, 1);
				System.arraycopy(content, 120, cip_rej_cause, 0, 1);
				System.arraycopy(content, 121, disconnect_cause, 0, 1);
				System.arraycopy(content, 122, cc_rel_cause, 0, 1);
				System.arraycopy(content, 123, clear_cause, 0, 1);
				System.arraycopy(content, 124, cp_cause, 0, 1);
				System.arraycopy(content, 125, rp_cause, 0, 1);
				System.arraycopy(content, 126, ho_cause, 0, 24);
				System.arraycopy(content, 150, ho_failure_cause, 0, 24);
				System.arraycopy(content, 174, rab_ass_failure_cause, 0, 2);
				System.arraycopy(content, 176, rab_rel_failure_cause, 0, 2);
				System.arraycopy(content, 178, rab_rel_request_cause, 0, 2);
				System.arraycopy(content, 180, iu_rel_request_cause, 0, 2);
				System.arraycopy(content, 182, iu_rel_command_cause, 0, 2);
				System.arraycopy(content, 184, first_paging_time, 0, 4);
				System.arraycopy(content, 188, second_paging_time, 0, 4);
				System.arraycopy(content, 192, third_paging_time, 0, 4);
				System.arraycopy(content, 196, fourth_paging_time, 0, 4);
				System.arraycopy(content, 200, cc_time, 0, 4);
				System.arraycopy(content, 204, rab_ass_time, 0, 4);
				System.arraycopy(content, 208, rab_ass_complete_time, 0, 4);
				System.arraycopy(content, 212, setup_time, 0, 4);
				System.arraycopy(content, 216, alert_time, 0, 4);
				System.arraycopy(content, 220, connect_time, 0, 4);
				System.arraycopy(content, 224, disconnect_time, 0, 4);
				System.arraycopy(content, 228, iu_release_request_time, 0, 4);
				System.arraycopy(content, 232, iu_release_command_time, 0, 4);
				System.arraycopy(content, 236, rp_data_time, 0, 4);
				System.arraycopy(content, 240, rp_ack_time, 0, 4);
				System.arraycopy(content, 244, auth_request_time, 0, 4);
				System.arraycopy(content, 248, auth_response_time, 0, 4);
				System.arraycopy(content, 252, sec_mode_cmd_time, 0, 4);
				System.arraycopy(content, 256, sec_mode_cmp_time, 0, 4);
				System.arraycopy(content, 260, cm_service_accept_time, 0, 4);
				System.arraycopy(content, 264, call_confirm_preceding_time, 0, 4);
				System.arraycopy(content, 268, connect_ack_time, 0, 4);
				System.arraycopy(content, 272, rab_release_request_time, 0, 4);
				System.arraycopy(content, 276, relocation_request_time, 0, 4);
				System.arraycopy(content, 280, relocation_request_ack_time, 0, 4);
				System.arraycopy(content, 284, relocation_command_time, 0, 4);
				System.arraycopy(content, 288, relocation_complete_time, 0, 4);
				System.arraycopy(content, 292, relocation_detect_time, 0, 4);
				System.arraycopy(content, 296, forward_srns_context_time, 0, 4);
				System.arraycopy(content, 300, smsc, 0, 24);
				System.arraycopy(content, 324, sm_type, 0, 1);
				System.arraycopy(content, 325, sm_data_coding_scheme, 0, 1);
				System.arraycopy(content, 326, sm_length, 0, 2);
				System.arraycopy(content, 328, rp_data_count, 0, 1);
				System.arraycopy(content, 329, handover_count, 0, 1);
				System.arraycopy(content, 330, info_trans_capability, 0, 1);
				System.arraycopy(content, 331, speech_version, 0, 1);
				System.arraycopy(content, 332, failed_handover_count, 0, 1);
				System.arraycopy(content, 333, sub_cdr_index_set, 0, 200);
				System.arraycopy(content, 533, sub_cdr_starttime_set, 0, 200);
				System.arraycopy(content, 733, call_stop, 0, 1);
				System.arraycopy(content, 734, interrnc_ho_count, 0, 1);
				System.arraycopy(content, 735, iu_release_cmp_time, 0, 4);
				System.arraycopy(content, 739, max_bitrate, 0, 4);
				System.arraycopy(content, 743, rab_release_dir, 0, 1);
				System.arraycopy(content, 744, bscid, 0, 8);
				System.arraycopy(content, 752, call_stop_msg, 0, 1);
				System.arraycopy(content, 753, call_stop_cause, 0, 1);
				System.arraycopy(content, 754, mscid, 0, 4);
				System.arraycopy(content, 758, last_bscid, 0, 8);
				System.arraycopy(content, 766, last_mscid, 0, 4);
				System.arraycopy(content, 770, dtmf, 0, 1);
				System.arraycopy(content, 771, cid, 0, 4);
				System.arraycopy(content, 775, last_cid, 0, 4);
				System.arraycopy(content, 779, tac, 0, 4);
				System.arraycopy(content, 783, cdr_rel_type, 0, 1);
				
				System.arraycopy(content, 784, identity_request_time , 0, 4);
				System.arraycopy(content, 788, identity_response_time , 0, 4);
				System.arraycopy(content, 792, ciph_mode_cmd_time , 0, 4);
				System.arraycopy(content, 796, ciph_mode_cmp_time , 0, 4);
				System.arraycopy(content, 800, tmsi_realloc_cmd_time , 0, 4);
				System.arraycopy(content, 804, tmsi_realloc_cmp_time , 0, 4);
				System.arraycopy(content, 808, cc_release_time  , 0, 4);
				
				System.arraycopy(content, 812, cc_release_cmp_time  , 0, 4);
				System.arraycopy(content, 816, clear_cmp_time  , 0, 4);
				System.arraycopy(content, 820, sccp_release_time  , 0, 4);
				System.arraycopy(content, 824, sccp_release_cmp_time   , 0, 4);

				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		
		public boolean Read(InputStream finput)
		{
			
			try {
				byte[] content = new byte[SIZE];
				finput.read(content, 0, SIZE);
				System.arraycopy(content, 0, start_time_s, 0, 4);
				System.arraycopy(content, 4, start_time_ns, 0, 4);
				System.arraycopy(content, 8, end_time_s, 0, 4);
				System.arraycopy(content, 12, end_time_ns, 0, 4);
				System.arraycopy(content, 16, cdr_index, 0, 4);
				System.arraycopy(content, 20, cdr_type, 0, 1);
				System.arraycopy(content, 21, cdr_result, 0, 1);
				System.arraycopy(content, 22, base_cdr_index, 0, 4);
				System.arraycopy(content, 26, base_cdr_type, 0, 1);
				System.arraycopy(content, 27, tmsi, 0, 4);
				System.arraycopy(content, 31, new_tmsi, 0, 4);
				System.arraycopy(content, 35, imsi, 0, 8);
				System.arraycopy(content, 43, imei, 0, 8);
				System.arraycopy(content, 51, calling_number, 0, 8);
				System.arraycopy(content, 59, called_number, 0, 8);
				System.arraycopy(content, 67, third_number, 0, 24);
				System.arraycopy(content, 91, mgw_ip, 0, 4);
				System.arraycopy(content, 95, msc_server_ip, 0, 4);
				System.arraycopy(content, 99, rnc_spc, 0, 4);
				System.arraycopy(content, 103, msc_spc, 0, 4);
				System.arraycopy(content, 107, lac, 0, 2);
				System.arraycopy(content, 109, ci, 0, 2);
				System.arraycopy(content, 111, last_lac, 0, 2);
				System.arraycopy(content, 113, last_ci, 0, 2);
				System.arraycopy(content, 115, cref_cause, 0, 1);
				System.arraycopy(content, 116, cm_rej_cause, 0, 1);
				System.arraycopy(content, 117, lu_rej_cause, 0, 1);
				System.arraycopy(content, 118, assign_failure_cause, 0, 1);
				System.arraycopy(content, 119, rr_cause, 0, 1);
				System.arraycopy(content, 120, cip_rej_cause, 0, 1);
				System.arraycopy(content, 121, disconnect_cause, 0, 1);
				System.arraycopy(content, 122, cc_rel_cause, 0, 1);
				System.arraycopy(content, 123, clear_cause, 0, 1);
				System.arraycopy(content, 124, cp_cause, 0, 1);
				System.arraycopy(content, 125, rp_cause, 0, 1);
				System.arraycopy(content, 126, ho_cause, 0, 24);
				System.arraycopy(content, 150, ho_failure_cause, 0, 24);
				System.arraycopy(content, 174, rab_ass_failure_cause, 0, 2);
				System.arraycopy(content, 176, rab_rel_failure_cause, 0, 2);
				System.arraycopy(content, 178, rab_rel_request_cause, 0, 2);
				System.arraycopy(content, 180, iu_rel_request_cause, 0, 2);
				System.arraycopy(content, 182, iu_rel_command_cause, 0, 2);
				System.arraycopy(content, 184, first_paging_time, 0, 4);
				System.arraycopy(content, 188, second_paging_time, 0, 4);
				System.arraycopy(content, 192, third_paging_time, 0, 4);
				System.arraycopy(content, 196, fourth_paging_time, 0, 4);
				System.arraycopy(content, 200, cc_time, 0, 4);
				System.arraycopy(content, 204, rab_ass_time, 0, 4);
				System.arraycopy(content, 208, rab_ass_complete_time, 0, 4);
				System.arraycopy(content, 212, setup_time, 0, 4);
				System.arraycopy(content, 216, alert_time, 0, 4);
				System.arraycopy(content, 220, connect_time, 0, 4);
				System.arraycopy(content, 224, disconnect_time, 0, 4);
				System.arraycopy(content, 228, iu_release_request_time, 0, 4);
				System.arraycopy(content, 232, iu_release_command_time, 0, 4);
				System.arraycopy(content, 236, rp_data_time, 0, 4);
				System.arraycopy(content, 240, rp_ack_time, 0, 4);
				System.arraycopy(content, 244, auth_request_time, 0, 4);
				System.arraycopy(content, 248, auth_response_time, 0, 4);
				System.arraycopy(content, 252, sec_mode_cmd_time, 0, 4);
				System.arraycopy(content, 256, sec_mode_cmp_time, 0, 4);
				System.arraycopy(content, 260, cm_service_accept_time, 0, 4);
				System.arraycopy(content, 264, call_confirm_preceding_time, 0, 4);
				System.arraycopy(content, 268, connect_ack_time, 0, 4);
				System.arraycopy(content, 272, rab_release_request_time, 0, 4);
				System.arraycopy(content, 276, relocation_request_time, 0, 4);
				System.arraycopy(content, 280, relocation_request_ack_time, 0, 4);
				System.arraycopy(content, 284, relocation_command_time, 0, 4);
				System.arraycopy(content, 288, relocation_complete_time, 0, 4);
				System.arraycopy(content, 292, relocation_detect_time, 0, 4);
				System.arraycopy(content, 296, forward_srns_context_time, 0, 4);
				System.arraycopy(content, 300, smsc, 0, 24);
				System.arraycopy(content, 324, sm_type, 0, 1);
				System.arraycopy(content, 325, sm_data_coding_scheme, 0, 1);
				System.arraycopy(content, 326, sm_length, 0, 2);
				System.arraycopy(content, 328, rp_data_count, 0, 1);
				System.arraycopy(content, 329, handover_count, 0, 1);
				System.arraycopy(content, 330, info_trans_capability, 0, 1);
				System.arraycopy(content, 331, speech_version, 0, 1);
				System.arraycopy(content, 332, failed_handover_count, 0, 1);
				System.arraycopy(content, 333, sub_cdr_index_set, 0, 200);
				System.arraycopy(content, 533, sub_cdr_starttime_set, 0, 200);
				System.arraycopy(content, 733, call_stop, 0, 1);
				System.arraycopy(content, 734, interrnc_ho_count, 0, 1);
				System.arraycopy(content, 735, iu_release_cmp_time, 0, 4);
				System.arraycopy(content, 739, max_bitrate, 0, 4);
				System.arraycopy(content, 743, rab_release_dir, 0, 1);
				System.arraycopy(content, 744, bscid, 0, 8);
				System.arraycopy(content, 752, call_stop_msg, 0, 1);
				System.arraycopy(content, 753, call_stop_cause, 0, 1);
				System.arraycopy(content, 754, mscid, 0, 4);
				System.arraycopy(content, 758, last_bscid, 0, 8);
				System.arraycopy(content, 766, last_mscid, 0, 4);
				System.arraycopy(content, 770, dtmf, 0, 1);
				System.arraycopy(content, 771, cid, 0, 4);
				System.arraycopy(content, 775, last_cid, 0, 4);
				System.arraycopy(content, 779, tac, 0, 4);
				System.arraycopy(content, 783, cdr_rel_type, 0, 1);
				
				System.arraycopy(content, 784, identity_request_time , 0, 4);
				System.arraycopy(content, 788, identity_response_time , 0, 4);
				System.arraycopy(content, 792, ciph_mode_cmd_time , 0, 4);
				System.arraycopy(content, 796, ciph_mode_cmp_time , 0, 4);
				System.arraycopy(content, 800, tmsi_realloc_cmd_time , 0, 4);
				System.arraycopy(content, 804, tmsi_realloc_cmp_time , 0, 4);
				System.arraycopy(content, 808, cc_release_time  , 0, 4);
				
				System.arraycopy(content, 812, cc_release_cmp_time  , 0, 4);
				System.arraycopy(content, 816, clear_cmp_time  , 0, 4);
				System.arraycopy(content, 820, sccp_release_time  , 0, 4);
				System.arraycopy(content, 824, sccp_release_cmp_time   , 0, 4);

				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			
		}

		public void AddWriteList(BlockingQueue queue) {
			try {
				queue.put(this);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

