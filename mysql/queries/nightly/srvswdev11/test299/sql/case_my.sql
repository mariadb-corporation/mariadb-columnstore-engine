
SELECT col2, CASE col2 WHEN "とうきょうと" THEN 'とうきょうと１' WHEN "東京都" THEN '東京都２' WHEN "トウキョウト" THEN 'トウキョウト３' WHEN "ﾄｳｷｮｳﾄ" THEN 'ﾄｳｷｮｳﾄ４'  ELSE 'それ以外' END  from case_tbl_my;

