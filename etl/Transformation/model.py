import pandera as pa
from pandera.typing import Series


class Stg_vehicle_information_schema(pa.SchemaModel):
    accident_index: Series[str]
    age_band_of_driver: Series[str]
    age_of_vehicle: Series[int]
    driver_home_area_type: Series[str]
    engine_capacity_cc: Series[int]
    hit_object_in_carriageway: Series[str]
    hit_object_off_carriageway: Series[str]
    journey_purpose_of_driver: Series[str]
    junction_location: Series[str]
    make: Series[str]
    model: Series[str]
    propulsion_code: Series[str]
    sex_of_driver: Series[str]
    skidding_and_overturning: Series[str]
    vehicle_leaving_carriageway: Series[str]
    vehicle_location_restricted_lane: Series[int]
    vehicle_manoeuvre: Series[str]
    vehicle_type: Series[str]
    was_vehicle_left_hand_drive: Series[str]
    x1st_point_of_impact: Series[str]
    year: Series[int]
    
    @pa.check("sex_of_driver", name="check gender")
    def check_sex_of_driver(cls, a: Series[str]) -> Series[bool]:
        return a.isin(['Female', 'Male', 'Not known', 'Data missing or out of range'])


class Stg_accident_information_schema(pa.SchemaModel):
    
    accident_index: Series[str] = pa.Field(unique=True)
    first_road_class: Series[str]
    first_road_number:  Series[float]
    second_road_class: Series[str]
    second_road_number: Series[float]
    accident_severity: Series[str]
    day_of_week: Series[str]
    junction_control: Series[str]
    junction_detail: Series[str]
    police_force: Series[str]
    road_surface_conditions: Series[str]
    road_type: Series[str]
    speed_limit: Series[float]
    urban_or_rural_area: Series[str]
    weather_conditions: Series[str]
    year: Series[int]
    time: Series[str]
    inscotland: Series[bool]
    
    @pa.check("day_of_week", name="check week day")
    def check_day_of_week(cls, a: Series[str]) -> Series[bool]:
        return a.isin(['Monday', 'Tuesday', 'Wednesday', "Thursday", "Friday", "Saturday", "Sunday"])