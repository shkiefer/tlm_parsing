import numpy as np
import pandas as pd
import struct
import base64
from io import BytesIO


def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        main_header_dics, supplemental_header_dics, data_dics = parse_tlm_file(BytesIO(decoded))
        df_hdr_main = pd.DataFrame(main_header_dics)
        df_hdr_supp = pd.DataFrame(supplemental_header_dics)
        df_data = assemble_tlm_data(data_dics)
        df_data['filename'] = filename
        df_hdr_main['filename'] = filename
        df_hdr_supp['filename'] = filename
    except Exception as e:
        print(e)
    return df_hdr_main, df_hdr_supp, df_data


def byte_to_bitstring(byte_data):
    """Converts a byte object to a string of 0s and 1s."""
    return ''.join(format(byte, '08b') for byte in byte_data)

def byte_to_bcd(byte):
    if isinstance(byte, bytes):
        return f"{byte[0] >> 4:x}{byte[0] & 0x0f:x}"
    else:
        return f"{byte >> 4:x}{byte & 0x0f:x}"

def bytes_to_bcd(bytes_data):
    """Converts bytes to a BCD string."""
    bcd_string = ""
    for byte in bytes_data:
        bcd_string += byte_to_bcd(byte)
    return bcd_string

def bcd_whole_and_decimel(bytes_data, whole_digits, lsb=True):
    if lsb:
        # reverse direction of bytes
        bytes_data = bytes_data[::-1]
    # whole number (left of decimel point)
    w = bytes_to_bcd(bytes_data)[:whole_digits]
    # decimel number (right of decimel point)
    d = bytes_to_bcd(bytes_data)[whole_digits:]        
    return w, d   


def parse_tlm_file(file_obj: BytesIO):

    """Summary
    Parse a Spektrum telemetry file.
    Supports parsing of main header, supplemental header, and the following data blocks  

    - PowerBox
    - Air Speed
    - Altitude
    - GForce
    - JetCat interface
    - GPS Location & Status
    - Standard Telemetry
    - RX Telemetry

    Returns:
        tuple: A tuple containing three lists:
            - main_header_dics (list): List of dictionaries containing main header information (1 per record session)
            - supplemental_header_dics (list): List of dictionaries containing supplemental header information (1 per sensor per record session).
            - data_dics (list): List of dictionaries containing parsed data blocks. ( 1 per sensor per timestamp)
    """

    main_header_dics = []
    supplemental_header_dics = []
    data_dics = []

    # read all bytes into memory
    with open(file_obj, 'rb') as f:
        file_bytes = f.read()
    
    session_id = 0
    i = 0
    while True:
        # headers are 36 bytes long, data are 20 bytes long
        if i >= len(file_bytes):
            break
        block_ts = file_bytes[i:i+4]
        
        if struct.unpack('<I', block_ts)[0] == 0xFFFFFFFF:
            # header block
            block = file_bytes[i:i+36]
            # TODO test if this is always true
            if (block[4] & 0xFF) != (block[5] & 0xFF) and b"'" in block:
                session_id += 1
                main_header_dics.append(
                    parse_main_header_block(block, session_id=session_id)
                )
            else:
                supplemental_header_dic = parse_supplemental_header_block(block, session_id=session_id)
                supplemental_header_dics.append(
                    supplemental_header_dic
                )
            # 36 byes in header block
            i += 36
        else:
            block = file_bytes[i:i+20]
            # parse_supplemental_header_block(block)
            data_dics.append(
                parse_data_block(block, session_id=session_id)
            )
            # 20 bytes in data block
            i += 20
    return main_header_dics, supplemental_header_dics, data_dics


def parse_main_header_block(block, session_id:int = 1):
    model_type_b = block[4:5]
    bind_info_b = block[5:6]
    # Model name may have 2 extra leading bytes if bind_info_b is 0xb2, and no leading bytes if bind_info_b is 0x00 (based on small test set)
    if bind_info_b == b'\xb2':
        model_name_b = block[12:22]
    elif bind_info_b == b'\x00':
        model_name_b = block[10:22]
    buffer_data_b = block[22:35]
    
    model_type = {b'\x00':'Fixed Wing', b'\x01': 'Helicopter', b'\02': 'Glider'}.get(model_type_b, 'Unknown')
    bind_info = {b'\xb2': 'DSMX 22ms', b'\x00': 'iXxx', b'\x01': 'DSM2 6000', b'\x02': 'DSM2 8000 RX', b'\03':'DSMX 8000 RX', b'\x04': 'DMSX 6000 RX'}.get(bind_info_b, 'Unknown')


    model_name = model_name_b.rstrip(b'\x00').decode('ascii')
    # TODO parse buffer data?
    buffer_data = buffer_data_b

    main_header_dic = {
        'session_id': session_id,
        'model_type': model_type,
        'bind_info': bind_info,
        'model_name': model_name,
        # 'buffer_data': buffer_data
    }

    return main_header_dic


def parse_supplemental_header_block(block, session_id:int=1):
    sensor_type_b = block[0x4], block[0x5]
    tel_setup_b = block[0x7:0x16]
    buffer_data_b = block[0x17:0x23]
    
    sensor_type = {
        (0x1, 0x01): 'Volt sensor', 
        (0x2, 0x02): 'Temp sensor',
        (0x3, 0x03): 'Amps Sensor',
        (0x0A, 0x0A): 'Power Box',
        (0x11, 0x11): 'Airspeed Sensor',
        (0x12, 0x12): 'Altitude Sensor',
        (0x14, 0x14): 'G-Force Sensor',
        (0x15, 0x15): 'JetCat Sensor',
        (0x16, 0x16): 'GPS Sensor',
        (0x17, 0x17): 'end of header',
        (0x7E, 0x7E): 'RPM Sensor',
        (0x7F, 0x7F): 'RX Telemetry',
        (0x20, 0x20): 'ESC Sensor',
        (0x1A, 0x1A): 'Gyro Sensor',
        (0x40, 0x40): 'Vario-S Sensor',
        (0x42, 0x42): 'Smart Battery',
        }.get(sensor_type_b, 'Unknown')
    # TODO parse buffer data?
    buffer_data = buffer_data_b
    # TODO parse tel_setup data?
    tel_setup = tel_setup_b

    supplemental_header_dic = {
        'session_id': session_id,
        'sensor_type': sensor_type,
        # 'tel_setup': tel_setup,
        # 'buffer_data': buffer_data
    }
    return supplemental_header_dic


def parse_data_block(block, session_id:int = 1):
    # Found by trial and error, total seconds, includes an initial offset value that must be subtracted to get relative time
    timestamp = int(struct.unpack('<I', block[:4])[0] * 10.)
    data_type = block[4]
    data_dic = {
        'session_id': session_id,
        'timestamp_ms': timestamp,
    }

    # specs for most data types are available below
    # https://www.spektrumrc.com/ProdInfo/Files/SPM_Telemetry_Developers_Specs.pdf
    if data_type == 0x00:
        data_name = 'No Data'  
        data_dic.update({'data_type': data_name}) 
    elif data_type == 0x01:
        data_name = 'High-Voltage (Internal)'
        data_dic.update({'data_type': data_name, 'data': parse_high_voltage_int(block)})
    elif data_type == 0x02:
        data_name = 'Temperature (Internal)'
        data_dic.update({'data_type': data_name, 'data': parse_temperature_int(block)})
    elif data_type == 0x0A:
        data_name = 'PowerBox'
        data_dic.update({'data_type': data_name, 'data': parse_powerbox(block)})
    elif data_type == 0x11:
        data_name = 'Air Speed'
        data_dic.update({'data_type': data_name, 'data': parse_airspeed(block)})
    elif data_type == 0x12:
        data_name = 'Altitude'
        data_dic.update({'data_type': data_name, 'data': parse_altitude(block)})
    elif data_type == 0x14:
        data_name = 'GForce'
        data_dic.update({'data_type': data_name, 'data': parse_gforce(block)})
    elif data_type == 0x15:
        data_name = 'JetCat interface'
        data_dic.update({'data_type': data_name, 'data': parse_jetcat_1(block)})
    elif data_type == 0x16:
        data_name = 'GPS Location'
        data_dic.update({'data_type': data_name, 'data': parse_gps_loc(block)})
    elif data_type == 0x17:
        data_name = 'GPS Status'
        data_dic.update({'data_type': data_name, 'data': parse_gps_stats(block)})
    elif data_type == 0x7E:
        data_name = 'Standard Receiver Telemetry'
        data_dic.update({'data_type': data_name, 'data': parse_standard_receiver_telemetry(block)})
    elif data_type == 0x7F:
        data_name = 'QoS'
        data_dic.update({'data_type': data_name, 'data': parse_QoS(block)})
    elif data_type == 0x20:
        data_name = 'ESC'
        data_dic.update({'data_type': data_name, 'data': parse_esc(block)})
    elif data_type == 0x1A:
        data_name = 'Gyro'
        data_dic.update({'data_type': data_name, 'data': parse_gyro(block)})
    elif data_type == 0x40:
        data_name = 'Vario-S'
        data_dic.update({'data_type': data_name, 'data': parse_variometer(block)})
    elif data_type == 0x42:
        data_name = 'Smart Battery'
        data_dic.update({'data_type': data_name, 'data': parse_smart_battery(block)})
    else:
        data_name = 'Unknown Source'
        data_dic.update({'data_type': data_name, 'data': 'Unknown Data Type'})

    return data_dic


def parse_high_voltage_int(block):
    return None


def parse_temperature_int(block):
    return None


def parse_powerbox(block):
    # Voltages are measured in 0.01v increments. Capacity is in units of 1mAh.
    sid = int(struct.unpack('B', block[5:6])[0])
    volt1 = float(struct.unpack('<H', block[6:8])[0]) * 0.01 if struct.unpack('<H', block[6:8])[0] != 0xFFFF else np.nan
    volt2 = float(struct.unpack('<H', block[8:10])[0]) * 0.01 if struct.unpack('<H', block[8:10])[0] != 0xFFFF else np.nan
    capacity1 = float(struct.unpack('<H', block[10:12])[0]) if struct.unpack('<H', block[10:12])[0] != 0xFFFF else np.nan
    capacity2 = float(struct.unpack('<H', block[12:14])[0]) if struct.unpack('<H', block[12:14])[0] != 0xFFFF else np.nan
    # unused spare data
    spare16_1 = struct.unpack('<H', block[14:16])[0]
    spare16_2 = struct.unpack('<H', block[16:18])[0]
    spare = struct.unpack('B', block[18:19])[0]

    alarms  = {0x1: 'Voltage_1', 0x2: 'Voltage_2', 0x4: 'Capacity_1', 0x8: 'Capacity_2', 0x10: 'RPM?', 0x20: 'Temperature?', 0x40: 'Reserved_1', 0x80: 'Reserved_2'}.get(block[19], 'Unknown')
    return {'pwrBox_sid': sid, 'pwrBox_volt1_v': volt1, 'pwrBox_volt2_v': volt2, 'pwrBox_capacity1_mAh': capacity1, 'pwrBox_capacity2_mAh': capacity2, 'pwrBox_alarms': alarms}


def parse_airspeed(block):
    # Airspeed measured in 1km/h increments.
    sid = int(struct.unpack('B', block[5:6])[0])
    airspeed = float(struct.unpack('<H', block[6:8])[0]) if struct.unpack('<H', block[6:8])[0] != 0xFFFF else np.nan
    airspeed_max = float(struct.unpack('<H', block[8:10])[0]) if struct.unpack('<H', block[8:10])[0] != 0xFFFF else np.nan
    return {'airSpeed_sid': sid, 'airSpeed_airspeed_km/h': airspeed, 'airSpeed_maxAirspeed_km/h': airspeed_max}


def parse_altitude(block):
    # Altitude in 0.1 meter increments
    sid = int(struct.unpack('B', block[5:6])[0])
    altitude = float(struct.unpack('>H', block[6:8])[0]) * 0.1 if struct.unpack('>H', block[6:8])[0] != 0xFFFF else np.nan
    altitude_max = float(struct.unpack('>H', block[8:10])[0]) * 0.1 if struct.unpack('>H', block[8:10])[0] != 0xFFFF else np.nan
    return {'alt_sid': sid, 'alt_altitude_m': altitude, 'alt_altitude_max_m': altitude_max}    


def parse_gforce(block):
    # Force is reported in 0.01G increments. 
    # The data type for these measurements are a 16-bit signed integer. 
    # Range is +/- 4000 (+/- 40G) in pro models. Range is +/- 800 (+/- 8G) in standard models. 
    # The max gforce for for the x-axis is the absolute value for fore/aft. 
    # The max gforce for the y-axis is the absolute value for left/right. 
    # The value for the z-axis is the wing spar load.
    sid = int(struct.unpack('B', block[5:6])[0])
    xg = float(struct.unpack('>h', block[6:8])[0]) * 0.01 if struct.unpack('>h', block[6:8])[0] != 0x7FFF else np.nan
    yg = float(struct.unpack('>h', block[8:10])[0]) * 0.01 if struct.unpack('>h', block[8:10])[0] != 0x7FFF else np.nan
    zg = float(struct.unpack('>h', block[10:12])[0]) * 0.01 if struct.unpack('>h', block[10:12])[0] != 0x7FFF else np.nan
    xg_abs_max = float(struct.unpack('>h', block[12:14])[0]) * 0.01 if struct.unpack('>h', block[12:14])[0] != 0x7FFF else np.nan
    yg_abs_max = float(struct.unpack('>h', block[14:16])[0]) * 0.01 if struct.unpack('>h', block[14:16])[0] != 0x7FFF else np.nan
    zg_max = float(struct.unpack('>h', block[16:18])[0]) * 0.01 if struct.unpack('>h', block[16:18])[0] != 0x7FFF else np.nan
    zg_min = float(struct.unpack('>h', block[18:20])[0]) * 0.01 if struct.unpack('>h', block[18:20])[0] != 0x7FFF else np.nan
    return {'gForce_sid': sid, 'gForce_x_g': xg, 'gForce_y_g': yg, 'gForce_z_g': zg, 'gForce_x_abs_max_g': xg_abs_max, 'gForce_y_abs_max_g': yg_abs_max, 'gForce_z_max_g': zg_max, 'gForce_z_min_g': zg_min}


def parse_jetcat_1(block):
    # TODO Implement parsing for jetcat data
    return None


def parse_gps_loc(block):
    sid = int(struct.unpack('B', block[5:6])[0])
    # altitudeLow
    w, d = bcd_whole_and_decimel(block[6:8], 3)
    altitudeLow = float(f'{w}.{d}')
    # latitude
    w, d = bcd_whole_and_decimel(block[8:12], 4)
    lat_deg = w[:2]
    lat_min = f'{w[2:]}.{d}'
    # decimel degrees
    latitude = float(lat_deg) + float(lat_min)/60.
    # longitude
    w, d = bcd_whole_and_decimel(block[12:16], 4)
    lon_deg = w[:2]
    lon_min = f'{w[2:4]}.{d}'
    # decimel degrees
    longitude = float(lon_deg) + float(lon_min)/60.
    # course
    w, d = bcd_whole_and_decimel(block[16:18], 3)
    course = float(f'{w}.{d}')
    # HDOP
    d = byte_to_bcd(block[18])
    hdop = float(f'{d[0]}.{d[1]}')
    # GPS flags
    gps_flags_bits = byte_to_bitstring(block[19:20])
    gps_flags = dict(
        gpsLoc_isNorth=gps_flags_bits[0] == '1',
        gpsLoc_isEast=gps_flags_bits[1] == '1',
        gpsLoc_isLongGT99=gps_flags_bits[2] == '1',
        gpsLoc_isGpsFixValid=gps_flags_bits[3] == '1',
        gpsLoc_isGpsDataReceived=gps_flags_bits[4] == '1',
        gpsLoc_is3Dfix=gps_flags_bits[5] == '1',
        gpsLoc_isNegAltitude=gps_flags_bits[6] == '1',
    )
    if gps_flags['gpsLoc_isLongGT99']:
        longitude = longitude + 100.
    if not gps_flags['gpsLoc_isEast']:
        longitude = longitude * -1

    return {'gpsLoc_sid': sid, 'gpsLoc_altitudeLow_m': altitudeLow, 'gpsLoc_latitude_deg': latitude, 'gpsLoc_longitude_deg': longitude, 'gpsLoc_course': course, 'gpsLoc_hdop': hdop, **gps_flags}


def parse_gps_stats(block):
    sid = int(struct.unpack('B', block[5:6])[0])
    # speed is in knots, 1.852 km/h
    # speed = 1.852 * (int(block[6]) / 10. + int(block[7]))
    w, d = bcd_whole_and_decimel(block[6:8], 3)
    speed = float(f'{w}.{d}') * 1.852
    # UTC time
    w, d = bcd_whole_and_decimel(block[8:12], 6)
    utc = f"{w[:2]}:{w[2:4]}:{w[4:6]}.{d}"
    utc_dt = pd.to_datetime(utc, format='%H:%M:%S.%f', errors='coerce', utc=True)
    numSats = int(byte_to_bcd(block[12:13]))
    # TODO test if this should be multiplied by 100 and added to gpsLoc_altitudeLow
    altitudeHigh = float(byte_to_bcd(block[13:14])) * 100
    return {'gpsStat_sid': sid, 'gpsStat_speed_km/h': speed, 'gpsStat_utc': utc, 'gpsStat_numSats': numSats, 'gpsStat_altitudeHigh_m': altitudeHigh}


def parse_standard_receiver_telemetry(block):
    sid = int(struct.unpack('B', block[5:6])[0])
    ms_ple = float(struct.unpack('>H', block[6:8])[0]) if struct.unpack('>H', block[6:8])[0] != 0xFFFF else np.nan
    volt = float(struct.unpack('>H', block[8:10])[0]) * 0.01 if struct.unpack('>H', block[8:10])[0] != 0xFFFF else np.nan
    temp_F = float(struct.unpack('>h', block[10:12])[0]) if struct.unpack('>h', block[10:12])[0] != 0x7FFF else np.nan
    dBm_A = float(struct.unpack('b', block[12:13])[0]) if struct.unpack('b', block[12:13])[0] != 0x7F else np.nan
    dBm_B = float(struct.unpack('b', block[13:14])[0]) if struct.unpack('b', block[13:14])[0] != 0x7F else np.nan
    return {'rcvr_sid': sid, 'rcvr_msPulse': ms_ple, 'rcvr_volt_v': volt, 'rcvr_temp_F': temp_F, 'rcvr_dBmA': dBm_A, 'rcvr_dBmB': dBm_B}


def parse_QoS(block):
    sid = int(struct.unpack('B', bytes(block[5:6]))[0])
    A = struct.unpack('>H', block[6:8])[0] if struct.unpack('>H', block[6:8])[0] != 0xFFFF else None
    B = struct.unpack('>H', block[8:10])[0] if struct.unpack('>H', block[8:10])[0] != 0xFFFF else None
    L = struct.unpack('>H', block[10:12])[0] if struct.unpack('>H', block[10:12])[0] != 0xFFFF else None
    R = struct.unpack('>H', block[12:14])[0] if struct.unpack('>H', block[12:14])[0] != 0xFFFF else None
    F = struct.unpack('>H', block[14:16])[0] if struct.unpack('>H', block[14:16])[0] != 0xFFFF else None
    H = struct.unpack('>H', block[16:18])[0] if struct.unpack('>H', block[16:18])[0] != 0xFFFF else None
    rxVolts = float(struct.unpack('>H', block[18:20])[0]) * 0.01 if struct.unpack('>H', block[18:20])[0] != 0xFFFF else np.nan
    return {'QoS_sid': sid, 'QoS_A': A, 'QoS_B': B, 'QoS_L': L, 'QoS_R': R, 'QoS_F': F, 'QoS_H': H, 'QoS_rxVolts_v': rxVolts}


def parse_esc(block):
    sid = int(struct.unpack('B', bytes(block[5:6]))[0])
    rpm = float(struct.unpack('>H', block[6:8])[0]) * 10 if struct.unpack('>H', block[6:8])[0] != 0xFFFF else np.nan
    v_input = float(struct.unpack('>H', block[8:10])[0]) * 0.01 if struct.unpack('>H', block[8:10])[0] != 0xFFFF else np.nan
    tempFET = float(struct.unpack('>H', block[10:12])[0]) * 0.1 if struct.unpack('>H', block[10:12])[0] != 0xFFFF else np.nan
    currentMotor = float(struct.unpack('>H', block[12:14])[0]) * 0.01 if struct.unpack('>H', block[12:14])[0] != 0xFFFF else np.nan
    tempBEC = float(struct.unpack('>H', block[14:16])[0]) * 0.1 if struct.unpack('>H', block[14:16])[0] != 0x7FFF else np.nan
    currentBEC = float(struct.unpack('B', block[16:17])[0]) * 0.1 if struct.unpack('B', block[16:17])[0] != 0xFF else np.nan
    v_BEC = float(struct.unpack('B', block[17:18])[0]) * 0.05 if struct.unpack('B', block[17:18])[0] != 0xFF else np.nan
    throttle = float(struct.unpack('B', block[18:19])[0]) * 0.5 if struct.unpack('B', block[18:19])[0] != 0xFF else np.nan
    powerOut = float(struct.unpack('B', block[19:20])[0]) * 0.5 if struct.unpack('B', block[19:20])[0] != 0xFF else np.nan
    return {'esc_sid': sid, 'esc_rpm': rpm, 'esc_vInput': v_input, 'esc_tempFET_C': tempFET, 'esc_currentMotor_amp': currentMotor, 'esc_tempBEC_C': tempBEC, 'esc_currentBEC_amp': currentBEC, 'esc_vBEC': v_BEC, 'esc_throttle_%': throttle, 'esc_powerOut_%': powerOut}


def parse_gyro(block):
    sid = int(struct.unpack('B', bytes(block[5:6]))[0])
    gyroX = float(struct.unpack('>h', block[6:8])[0]) * 0.1 if struct.unpack('>h', block[6:8])[0] != 0x7FFF else np.nan
    gyroY = float(struct.unpack('>h', block[8:10])[0]) * 0.1 if struct.unpack('>h', block[8:10])[0] != 0x7FFF else np.nan
    gyroZ = float(struct.unpack('>h', block[10:12])[0]) * 0.1 if struct.unpack('>h', block[10:12])[0] != 0x7FFF else np.nan
    gyroX_abs_max = float(struct.unpack('>h', block[12:14])[0]) * 0.1 if struct.unpack('>h', block[12:14])[0] != 0x7FFF else np.nan
    gyroY_abs_max = float(struct.unpack('>h', block[14:16])[0]) * 0.1 if struct.unpack('>h', block[14:16])[0] != 0x7FFF else np.nan
    gyroZ_abs_max = float(struct.unpack('>h', block[16:18])[0]) * 0.1 if struct.unpack('>h', block[16:18])[0] != 0x7FFF else np.nan
    return {'gyro_sid': sid, 'gyro_gyroX_deg/s': gyroX, 'gyro_gyroY_deg/s': gyroY, 'gyro_gyroZ_deg/s': gyroZ, 'gyro_gyroX_abs_max_deg/s': gyroX_abs_max, 'gyro_gyroY_abs_max_deg/s': gyroY_abs_max, 'gyro_gyroZ_abs_max_deg/s': gyroZ_abs_max}


def parse_variometer(block):
    sid = int(struct.unpack('B', bytes(block[5:6]))[0])
    altitude = float(struct.unpack('>h', block[6:8])[0]) * 0.1 if struct.unpack('>h', block[6:8])[0] != 0x7FFF else np.nan
    delta_0250ms = float(struct.unpack('>h', block[8:10])[0]) * 0.1 if struct.unpack('>h', block[8:10])[0] != 0x7FFF else np.nan
    delta_0500ms = float(struct.unpack('>h', block[10:12])[0]) * 0.1 if struct.unpack('>h', block[10:12])[0] != 0x7FFF else np.nan
    delta_1000ms = float(struct.unpack('>h', block[12:14])[0]) * 0.1 if struct.unpack('>h', block[12:14])[0] != 0x7FFF else np.nan
    delta_1500ms = float(struct.unpack('>h', block[14:16])[0]) * 0.1 if struct.unpack('>h', block[14:16])[0] != 0x7FFF else np.nan
    delta_2000ms = float(struct.unpack('>h', block[16:18])[0]) * 0.1 if struct.unpack('>h', block[16:18])[0] != 0x7FFF else np.nan
    delta_3000ms = float(struct.unpack('>h', block[18:20])[0]) * 0.1 if struct.unpack('>h', block[18:20])[0] != 0x7FFF else np.nan
    return {'vario_sid': sid, 'vario_altitude_m': altitude, 'vario_delta_0250ms_m/s': delta_0250ms, 'vario_delta_0500ms_m/s': delta_0500ms, 'vario_delta_1000ms_m/s': delta_1000ms, 'vario_delta_1500ms_m/s': delta_1500ms, 'vario_delta_2000ms_m/s': delta_2000ms, 'vario_delta_3000ms_m/s': delta_3000ms}

def parse_smart_battery(block):
    sid = int(struct.unpack('B', bytes(block[5:6]))[0])
    type_Channel = int(struct.unpack('B', bytes(block[6:7]))[0])
    if type_Channel == 0:
        # Real Time Data
        temp_C = float(struct.unpack('b', block[7:8])[0]) if struct.unpack('b', block[7:8])[0] != 0x7F else np.nan
        dichargeCurrent_mA = float(struct.unpack('<L', block[8:12])[0]) if struct.unpack('<L', block[8:12])[0] != 0xFFFFFFFF else np.nan
        battCapacityUse = float(struct.unpack('<H', block[12:14])[0]) if struct.unpack('<H', block[12:14])[0] != 0xFFFF else np.nan
        minCellVoltage_v = float(struct.unpack('<H', block[14:16])[0]) / 1000. if struct.unpack('<H', block[14:16])[0] != 0xFFFF else np.nan
        maxCellVoltage_v = float(struct.unpack('<H', block[16:18])[0]) / 1000. if struct.unpack('<H', block[16:18])[0] != 0xFFFF else np.nan
        return {'smartBatRT_sid': sid, 'smartBatRT_temp_C': temp_C, 'smartBatRT_dichargeCurrent_mA': dichargeCurrent_mA, 'smartBatRT_battCapacityUse_mAh': battCapacityUse, 'smartBatRT_minCellVoltage_v': minCellVoltage_v, 'smartBatRT_maxCellVoltage_v': maxCellVoltage_v}

    elif type_Channel == 16:
        # Cell Voltage Data
        temp_C = float(struct.unpack('b', block[6:7])[0]) if struct.unpack('b', block[6:7])[0] != 0x7F else np.nan
        cell1 = float(struct.unpack('<H', block[7:9])[0]) / 1000. if struct.unpack('<H', block[7:9])[0] != 0xFFFF else np.nan
        cell2 = float(struct.unpack('<H', block[9:11])[0]) / 1000. if struct.unpack('<H', block[9:11])[0] != 0xFFFF else np.nan
        cell3 = float(struct.unpack('<H', block[11:13])[0]) / 1000. if struct.unpack('<H', block[11:13])[0] != 0xFFFF else np.nan
        cell4 = float(struct.unpack('<H', block[13:15])[0]) / 1000. if struct.unpack('<H', block[13:15])[0] != 0xFFFF else np.nan
        cell5 = float(struct.unpack('<H', block[15:17])[0]) / 1000. if struct.unpack('<H', block[15:17])[0] != 0xFFFF else np.nan
        cell6 = float(struct.unpack('<H', block[17:19])[0]) / 1000. if struct.unpack('<H', block[17:19])[0] != 0xFFFF else np.nan
        
        return {'smartBatCells_sid': sid, 'smartBatCells_temp_C': temp_C, 'smartBatCells_cell1_v': cell1, 'smartBatCells_cell2_v': cell2, 'smartBatCells_cell3_v': cell3, 'smartBatCells_cell4_v': cell4, 'smartBatCells_cell5_v': cell5, 'smartBatCells_cell6_v': cell6}
    else:
        return {}
    

def assemble_tlm_data(data_dics):
    """Summary
    Assemble telemetry data into a Pandas dataframe.
    Args:
        data_dics (list): List of dictionaries containing parsed data blocks. ( 1 per sensor per timestamp)
    Returns:
        pandas.DataFrame: DataFrame containing telemetry data.
    """

    session_ids = sorted(set([d.get('session_id') for d in data_dics]))

    # treat each session as unique
    dfs_all = []
    for session_id in session_ids:
        dfs = []
        for i, data_type in enumerate(set([d.get('data_type') for d in data_dics if d.get('data_type') != 'Unknown Source' and d.get('data_type') != 'No Data' and d.get('session_id') == session_id])):
            dfi = pd.DataFrame([d.get('data') for d in data_dics if d['data_type'] == data_type and d.get('session_id') == session_id])
            dfi['timestamp_ms'] = [d.get('timestamp_ms') for d in data_dics if d['data_type'] == data_type and d.get('session_id') == session_id]
            
            # special handling of known nullable integer columns
            if data_type == 'QoS':
                for c in ['QoS_A', 'QoS_B', 'QoS_L', 'QoS_R', 'QoS_F', 'QoS_H']:
                    if c in dfi.columns:
                        dfi[c] = dfi[c].astype('Int64')
            # handle utc time from GPS
            if data_type == 'GPS Status':
                dfi['gpsStat_utc'] = pd.to_datetime(dfi['gpsStat_utc'], format='%H:%M:%S.%f', errors='coerce', utc=True)

            dfi = dfi.drop_duplicates(keep='first', subset='timestamp_ms').set_index('timestamp_ms', drop=True).sort_index()
            dfs.append(dfi)  
        # handle case when no known data is in set
        if len(dfs) == 0:
            continue
        df = pd.concat(dfs, axis=1, ignore_index=False).sort_index()
        df = df.reset_index(drop=False)
        df['Session ID'] = session_id
        df['Elapsed Time (s)'] = (df['timestamp_ms'] - df['timestamp_ms'].shift(1)).fillna(0).cumsum() / 1000.
        dfs_all.append(df.reset_index(drop=True))
    df_all = pd.concat(dfs_all, axis=0, ignore_index=True)
    return df_all