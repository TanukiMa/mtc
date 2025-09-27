-- 複数のワーカーが重複しないように、未発見のURLを取得し、
-- 同時にステータスを'discovering'に更新する関数
CREATE OR REPLACE FUNCTION get_and_lock_undiscovered_urls(limit_count integer)
RETURNS SETOF crawl_queue AS $$
BEGIN
  RETURN QUERY
  WITH updated_rows AS (
    UPDATE crawl_queue
    SET discovery_status = 'discovering'
    WHERE id IN (
      SELECT id
      FROM crawl_queue
      WHERE discovery_status = 'undiscovered'
      LIMIT limit_count
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  )
  SELECT * FROM updated_rows;
END;
$$ LANGUAGE plpgsql;
