class ItemDatasetQuery:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def item_dataset_df(self, user_id):
        """
        공개 포트폴리오용 샘플 쿼리입니다.
        실제 운영 DB 테이블명은 sample_* 테이블명으로 치환했습니다.
        """
        query = f"""
        SELECT
            REGEXP_REPLACE(sample_sector.code, '[0-9]', '') AS "Area",
            sample_delivery_item.tracking_number,
            sample_address.lat AS "lat",
            sample_address.lng AS "lng",
            sample_address.id AS "address_id",
            sample_address.address_road,
            sample_delivery_timetable.timestamp_out_for_delivery
        FROM sample_delivery_item
            JOIN sample_delivery_timetable
              ON sample_delivery_item.id = sample_delivery_timetable.delivery_item_id
            JOIN sample_delivery_container
              ON sample_delivery_item.container_id = sample_delivery_container.id
            JOIN sample_sector
              ON sample_delivery_item.designated_sector_id = sample_sector.id
            JOIN sample_address
              ON sample_delivery_item.address_id = sample_address.id
        WHERE sample_delivery_container.user_id = {int(user_id)}
          AND sample_delivery_timetable.timestamp_delivery_complete IS NULL
          AND sample_delivery_timetable.timestamp_return_collected IS NULL
          AND sample_delivery_container.timestamp_checkin IS NOT NULL
          AND DATE_ADD(sample_delivery_container.timestamp_checkin, INTERVAL 9 HOUR)
              >= CONCAT(DATE(DATE_ADD(NOW(), INTERVAL 9 HOUR)), ' 10:00:00')
          AND DATE_ADD(sample_delivery_container.timestamp_checkin, INTERVAL 9 HOUR)
              <= CONCAT(DATE_ADD(DATE(DATE_ADD(NOW(), INTERVAL 9 HOUR)), INTERVAL 1 DAY), ' 03:00:00')
        """
        return self.db_handler.fetch_data("sample_app", query, query_name="item_dataset_df")

    def user_id_by_tracking_number(self, tracking_number):
        query = f"""
        SELECT
            sc.user_id
        FROM sample_delivery_item s
        JOIN sample_delivery_container sc
          ON s.container_id = sc.id
        WHERE s.tracking_number = '{tracking_number}'
        ORDER BY sc.timestamp_checkin DESC
        LIMIT 1
        """
        return self.db_handler.fetch_data("sample_app", query, query_name="user_id_by_tracking_number")
