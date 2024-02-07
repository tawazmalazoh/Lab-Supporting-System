from django import template
from datetime import datetime
import pytz

register = template.Library()

@register.filter
def get_attr_with_underscore(row, attr):
    return row.get(attr, None)

@register.filter
def get_attr(obj, attr):
    return getattr(obj, attr)

@register.filter
def time_of_day(value):
    if isinstance(value, str):  # If the input is a string, assume it's a date/time string
        try:
            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return "Invalid Date/Time"

    if isinstance(value, (int, float)):
        # If the input is an integer or a float, assume it's a timestamp
        try:
            value = datetime.fromtimestamp(value)
        except (ValueError, OSError):
            return "Invalid Timestamp"

    if not isinstance(value, datetime):
        return "Invalid Date/Time"

    # Convert the datetime to the desired timezone (+2:00)
    timezone = pytz.timezone('Etc/GMT+2')  # Change the timezone as needed
    value = value.astimezone(timezone)

    hour = value.hour
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 18:
        return "Afternoon"
    else:
        return "Evening"
    
    
    
@register.filter(name='get_item')
def get_item(dictionary, key):
    return dictionary.get(key)

@register.filter(name='safe_get_item')
def safe_get_item(value, key):
    if isinstance(value, dict):
        return value.get(key)
    elif isinstance(value, str):
        # Handle strings or return a default value or message
        return value if key == 'value' else None
    return None



@register.filter(name='format_datetime')
def format_datetime(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f')
    
 

@register.filter
def get_value_from_list(data_list, platform):
    return data_list, platform  # Return both the list and the platform for the next filter

@register.filter
def extract_value(platform_date_tuple, date):
    data_list, platform = platform_date_tuple
    for row in data_list:
        if row['Platform'] == platform:
            return row.get(date)
    return None



@register.filter
def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value

