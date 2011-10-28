package Data;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import Format.BytesToString;

public class BICCCDR implements WriteObject {
	byte[] start_time_s = new byte[4];
	byte[] start_time_ns = new byte[4];
	byte[] end_time_s = new byte[4];
	byte[] end_time_ns = new byte[4];
	byte[] cdr_index = new byte[4];
	byte[] source_ip = new byte[4];
	byte[] destination_ip = new byte[4];
	byte[] cic = new byte[4];
	byte[] opc = new byte[4];
	byte[] dpc = new byte[4];
	byte[] release_reason = new byte[1];
	byte[] calling_party_number = new byte[8];
	byte[] called_party_number = new byte[24];
	byte[] original_called_number = new byte[24];
	byte[] transfer_number = new byte[24];
	byte[] location_number = new byte[24];
	byte[] response_time = new byte[4];
	byte[] acm_time = new byte[4];
	byte[] anm_time = new byte[4];
	byte[] rel_time = new byte[4];
	byte[] call_duration = new byte[4];
	byte[] duration = new byte[4];
	byte[] codec_modify_flag = new byte[1];
	byte[] codec_modify_result = new byte[1];
	byte[] codec_negotiation_flag = new byte[1];
	byte[] codec_negotiation_result = new byte[1];
	byte[] codec_type = new byte[4];
	byte[] call_type = new byte[1];
	byte[] call_hold = new byte[1];
	byte[] call_forward = new byte[1];
	byte[] call_waiting = new byte[1];
	byte[] confrence_call = new byte[1];
	byte[] is_ext_platform = new byte[1];

	public static final int SIZE =  183;
	
	public BICCCDR() {
	}

	public boolean decode(byte[] content) {

		try {
			System.arraycopy(content, 0, start_time_s, 0, 4);
			System.arraycopy(content, 4, start_time_ns, 0, 4);
			System.arraycopy(content, 8, end_time_s, 0, 4);
			System.arraycopy(content, 12, end_time_ns, 0, 4);
			System.arraycopy(content, 16, cdr_index, 0, 4);
			System.arraycopy(content, 20, source_ip, 0, 4);
			System.arraycopy(content, 24, destination_ip, 0, 4);
			System.arraycopy(content, 28, cic, 0, 4);
			System.arraycopy(content, 32, opc, 0, 4);

			System.arraycopy(content, 36, dpc, 0, 4);
			System.arraycopy(content, 40, release_reason, 0, 1);
			System.arraycopy(content, 41, calling_party_number, 0, 8);
			System.arraycopy(content, 49, called_party_number, 0, 24);
			System.arraycopy(content, 73, original_called_number, 0, 24);
			System.arraycopy(content, 97, transfer_number, 0, 24);

			System.arraycopy(content, 121, location_number, 0, 24);
			System.arraycopy(content, 145, response_time, 0, 4);
			System.arraycopy(content, 149, acm_time, 0, 4);
			System.arraycopy(content, 153, anm_time, 0, 4);
			System.arraycopy(content, 157, rel_time, 0, 4);
			System.arraycopy(content, 161, call_duration, 0, 4);

			System.arraycopy(content, 165, duration, 0, 4);
			System.arraycopy(content, 169, codec_modify_flag, 0, 1);
			System.arraycopy(content, 170, codec_modify_result, 0, 1);
			System.arraycopy(content, 171, codec_negotiation_flag, 0, 1);
			System.arraycopy(content, 172, codec_negotiation_result, 0, 1);
			System.arraycopy(content, 173, codec_type, 0, 4);

			System.arraycopy(content, 177, call_type, 0, 1);
			System.arraycopy(content, 178, call_hold, 0, 1);
			System.arraycopy(content, 179, call_forward, 0, 1);
			System.arraycopy(content, 180, call_waiting, 0, 1);
			System.arraycopy(content, 181, confrence_call, 0, 1);
			System.arraycopy(content, 182, is_ext_platform, 0, 1);
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

	public String getSource_ip() {
		return BytesToString.Bytes4ToString(source_ip);
	}

	public String getDestination_ip() {
		return BytesToString.Bytes4ToString(destination_ip);
	}

	public String getCic() {
		return BytesToString.Bytes4ToString(cic);
	}

	public String getOpc() {
		return BytesToString.Bytes4ToString(opc);
	}

	public String getDpc() {
		return BytesToString.Bytes4ToString(dpc);
	}

	public String getRelease_reason() {
		return BytesToString.Byte1ToString(release_reason);
	}

	public String getCalling_party_number() {
		return BytesToString.Bytes8toString(calling_party_number);
	}

	public String getCalled_party_number() {
		return BytesToString.CharstoString(called_party_number);
	}

	public String getOriginal_called_number() {
		return BytesToString.CharstoString(original_called_number);
	}

	public String getTransfer_number() {
		return BytesToString.CharstoString(transfer_number);
	}

	public String getLocation_number() {
		return BytesToString.CharstoString(location_number);
	}

	public String getResponse_time() {
		return BytesToString.Bytes4ToString(response_time);
	}

	public String getAcm_time() {
		return BytesToString.Bytes4ToString(acm_time);
	}

	public String getAnm_time() {
		return BytesToString.Bytes4ToString(anm_time);
	}

	public String getRel_time() {
		return BytesToString.Bytes4ToString(rel_time);
	}

	public String getCall_duration() {
		return BytesToString.Bytes4ToString(call_duration);
	}

	public String getDuration() {
		return BytesToString.Bytes4ToString(duration);
	}

	public String getCodec_modify_flag() {
		return BytesToString.Byte1ToString(codec_modify_flag);
	}

	public String getCodec_modify_result() {
		return BytesToString.Byte1ToString(codec_modify_result);
	}

	public String getCodec_negotiation_flag() {
		return BytesToString.Byte1ToString(codec_negotiation_flag);
	}

	public String getCodec_negotiation_result() {
		return BytesToString.Byte1ToString(codec_negotiation_result);
	}

	public String getCodec_type() {
		return BytesToString.Bytes4ToString(codec_type);
	}

	public String getCall_type() {
		return BytesToString.Byte1ToString(call_type);
	}

	public String getCall_hold() {
		return BytesToString.Byte1ToString(call_hold);
	}

	public String getCall_forward() {
		return BytesToString.Byte1ToString(call_forward);
	}

	public String getCall_waiting() {
		return BytesToString.Byte1ToString(call_waiting);
	}

	public String getConfrence_call() {
		return BytesToString.Byte1ToString(confrence_call);
	}

	public String getIs_ext_platform() {
		return BytesToString.Byte1ToString(is_ext_platform);
	}
	public void 	AddWriteList(BlockingQueue queue)
	{
		
	}
	public void 	Write(FileOutputStream out)
	{
		
	}
	public boolean ReadCDRFromHDFS(FSDataInputStream stream)
	{
		return false;
	}
	public boolean Read( InputStream finput)
	{
		return true;
	}
	

}
