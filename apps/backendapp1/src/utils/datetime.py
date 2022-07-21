import pendulum

def validate_date_from_date_to(date_from, date_to):
    def _process(date):
        if date is None:
            return None
        if isinstance(date, str):
            try: 
                return pendulum.parse(date)
            except:
                return None
        return date

    date_from = _process(date_from)
    date_to = _process(date_to)

    if date_from and date_to:
        if date_to > date_from:
            return date_from, date_to
        else:
            return date_to, date_from
    
    return date_from, date_to
