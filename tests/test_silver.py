import pytest
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from src.collectors.silver import clean_flight_time, to_dt, process_silver_layer

KST = ZoneInfo("Asia/Seoul")

def test_clean_flight_time():
    # 1. 정상적인 경우
    assert clean_flight_time("13:45") == "1345"
    
    # 2. 콜론 뒤 분이 60분 이상인 경우
    assert clean_flight_time("13:68") == "1408"
    
    # 3. 3자리수인 경우
    assert clean_flight_time("9:15") == "0915"
    assert clean_flight_time("915") == "0915"
    
    # 4. 24시간 이상인 경우
    assert clean_flight_time("25:10") == "0110"
    
    # 5. 유효하지 않은 경우
    assert clean_flight_time(None) is None
    assert clean_flight_time("") is None
    assert clean_flight_time("invalid") is None
    assert clean_flight_time("12") is None # Too short

def test_to_dt():
    # 2024-03-01 13:45 KST -> 04:45 UTC
    row = {'ymd': '20240301', 'time': '1345'}
    result = to_dt(row, 'time')
    
    assert result.year == 2024
    assert result.month == 3
    assert result.day == 1
    assert result.hour == 4
    assert result.minute == 45
    assert str(result.tzinfo) == 'UTC'

def test_process_silver_layer_deduplication(mocker):
    # 빅쿼리 읽기, 저장, MERGE 쿼리 실행을 위한 mock
    mock_read_gbq = mocker.patch("pandas.read_gbq")
    mock_load_to_bq = mocker.patch("src.collectors.silver.load_df_to_bq")
    mock_get_client = mocker.patch("src.collectors.silver.get_bq_client")
    
    # 3개의 스냅샷 데이터 준비
    # 1. 첫 스냅샷
    # 2. 두 번째 스냅샷 (예정 시간, 상태 변경 없음) -> 제외
    # 3. 세 번째 스냅샷 (예정 시간 변경) -> 유지
    data = [
        {
            'flight_key': 'FL123',
            'scheduled_time': '10:00',
            'expected_time': '10:00',
            'actual_time': None,
            'status': 'Scheduled',
            'collected_at': pd.Timestamp('2024-03-01 08:00:00'),
            'ymd': '20240301',
            'airline_icao': 'ABC',
            'flight_iata': 'AB123'
        },
        {
            'flight_key': 'FL123',
            'scheduled_time': '10:00',
            'expected_time': '10:00',
            'actual_time': None,
            'status': 'Scheduled',
            'collected_at': pd.Timestamp('2024-03-01 08:10:00'),
            'ymd': '20240301',
            'airline_icao': 'ABC',
            'flight_iata': 'AB123'
        },
        {
            'flight_key': 'FL123',
            'scheduled_time': '10:00',
            'expected_time': '10:30',
            'actual_time': None,
            'status': 'Delayed',
            'collected_at': pd.Timestamp('2024-03-01 08:20:00'),
            'ymd': '20240301',
            'airline_icao': 'ABC',
            'flight_iata': 'AB123'
        }
    ]
    mock_read_gbq.return_value = pd.DataFrame(data)
    
    # Silver layer 처리 실행
    process_silver_layer(ymd_list=['20240301'])
    
    # load_df_to_bq가 호출되었는지 확인
    assert mock_load_to_bq.called
    args, kwargs = mock_load_to_bq.call_args
    final_df = args[0]
    
    assert len(final_df) == 2
    assert final_df.iloc[0]['collected_at'] == pd.Timestamp('2024-03-01 08:00:00')
    assert final_df.iloc[1]['collected_at'] == pd.Timestamp('2024-03-01 08:20:00')

def test_process_silver_layer_empty_bronze(mocker):
    """
    Bronze 데이터가 없을 때 빠르게 early return하는지 검증합니다.
    """
    mock_read_gbq = mocker.patch("pandas.read_gbq")
    mock_load_to_bq = mocker.patch("src.collectors.silver.load_df_to_bq")
    
    # 빈 DataFrame 반환
    mock_read_gbq.return_value = pd.DataFrame()
    
    process_silver_layer(ymd_list=['20240301'])
    
    # BQ 업로드가 호출되지 않아야 함
    assert not mock_load_to_bq.called

def test_process_silver_layer_query_build(mocker):
    """
    ymd_list 값이 여러 개일 때 올바른 쿼리가 생성되는지 검증합니다.
    """
    mock_read_gbq = mocker.patch("pandas.read_gbq")
    mock_load_to_bq = mocker.patch("src.collectors.silver.load_df_to_bq")
    
    # Early return을 위해 빈 데이터 반환
    mock_read_gbq.return_value = pd.DataFrame()
    
    process_silver_layer(ymd_list=['20240301', '20240302'])
    
    # pandas.read_gbq에 전달된 query 확인
    call_args = mock_read_gbq.call_args[0]
    query = call_args[0]
    
    assert "ymd IN ('20240301', '20240302')" in query

def test_process_silver_layer_merge_exception(mocker):
    """
    MERGE 중 예외 발생 시 적절하게 로깅하고 예외를 다시 발생시키는지 검증합니다.
    """
    mock_read_gbq = mocker.patch("pandas.read_gbq")
    mock_load_to_bq = mocker.patch("src.collectors.silver.load_df_to_bq")
    mock_get_client = mocker.patch("src.collectors.silver.get_bq_client")
    
    data = [
        {
            'flight_key': 'FL123',
            'scheduled_time': '10:00',
            'expected_time': '10:00',
            'actual_time': None,
            'status': 'Scheduled',
            'collected_at': pd.Timestamp('2024-03-01 08:00:00'),
            'ymd': '20240301',
            'airline_icao': 'ABC',
            'flight_iata': 'AB123'
        }
    ]
    mock_read_gbq.return_value = pd.DataFrame(data)
    
    # MERGE 쿼리를 실행할 때 예외를 발생시킴
    mock_client_instance = mocker.MagicMock()
    mock_client_instance.query.side_effect = Exception("MERGE FAILED")
    mock_get_client.return_value = mock_client_instance
    
    with pytest.raises(Exception, match="MERGE FAILED"):
        process_silver_layer(ymd_list=['20240301'])
