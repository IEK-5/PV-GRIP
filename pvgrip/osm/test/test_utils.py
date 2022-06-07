from pvgrip.osm.utils import is_file_valid_osm

def test_is_file_valid_osm():
    error_file = "overpass_error.html"
    osm_file = "test.osm"
    osm_file_wrong_ext = "test_osm.wrong_extension"
    error_file_wrong_ext = "overpass_error_but_osm_ext.osm"
    assert not is_file_valid_osm(error_file)
    assert is_file_valid_osm(osm_file)
    assert is_file_valid_osm(osm_file_wrong_ext)
    assert not is_file_valid_osm(error_file_wrong_ext)
    
if __name__ == "__main__" :
    test_is_file_valid_osm()
