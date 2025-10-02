
def timestamp_to_iso_format():
    async def action(data):
        for entry in data:
            entry["timestamp"] = entry["timestamp"].isoformat()
        return data
    return action
