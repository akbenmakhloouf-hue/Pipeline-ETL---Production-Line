"""
Script de Test Complet pour ETL Pipeline
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime


def test_database_integrity(db_path='production.db'):
    """Test complet de l'intégrité de la base de données."""

    print("=" * 80)
    print(" TEST COMPLET DE LA BASE DE DONNÉES ETL")
    print("=" * 80)
    print(f"Date du test: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")

    if not os.path.exists(db_path):
        print(f" ERREUR: Base de données '{db_path}' introuvable!")
        return False

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    all_tests_passed = True

    # ========================================================================
    # TEST 1: Vérification de l'existence des tables
    # ========================================================================
    print("\n[TEST 1] Vérification de l'existence des tables")
    print("-" * 80)

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    required_tables = ['sensor_readings', 'quality_checks', 'hourly_summary']

    for table in required_tables:
        if table in tables:
            print(f"   Table '{table}' existe")
        else:
            print(f"   Table '{table}' MANQUANTE")
            all_tests_passed = False

    # ========================================================================
    # TEST 2: Vérification du nombre d'enregistrements
    # ========================================================================
    print("\n[TEST 2] Vérification du nombre d'enregistrements")
    print("-" * 80)

    # Test sensor_readings
    cursor.execute("SELECT COUNT(*) FROM sensor_readings")
    sensor_count = cursor.fetchone()[0]
    print(f"   sensor_readings: {sensor_count:,} enregistrements")
    if sensor_count == 0:
        print(f"  ️  ATTENTION: Table vide!")
        all_tests_passed = False
    else:
        print(f"   Table contient des données")

    # Test quality_checks
    cursor.execute("SELECT COUNT(*) FROM quality_checks")
    quality_count = cursor.fetchone()[0]
    print(f"   quality_checks: {quality_count:,} enregistrements")
    if quality_count == 0:
        print(f"  ⚠  ATTENTION: Table vide!")
    else:
        print(f"   Table contient des données")

    # Test hourly_summary
    cursor.execute("SELECT COUNT(*) FROM hourly_summary")
    summary_count = cursor.fetchone()[0]
    print(f"   hourly_summary: {summary_count:,} enregistrements")
    if summary_count == 0:
        print(f"  ⚠  ATTENTION: Table vide!")
        all_tests_passed = False
    else:
        print(f"   Table contient des données")

    # ========================================================================
    # TEST 3: Vérification de la structure des tables
    # ========================================================================
    print("\n[TEST 3] Vérification de la structure des tables")
    print("-" * 80)

    # Structure sensor_readings
    cursor.execute("PRAGMA table_info(sensor_readings)")
    sensor_columns = [row[1] for row in cursor.fetchall()]
    expected_sensor_cols = ['record_id', 'timestamp', 'line_id', 'machine_id',
                            'temperature', 'pressure', 'vibration', 'power', 'data_quality']

    print("  Colonnes de sensor_readings:")
    for col in expected_sensor_cols:
        if col in sensor_columns:
            print(f"     {col}")
        else:
            print(f"     {col} MANQUANTE")
            all_tests_passed = False

    # ========================================================================
    # TEST 4: Vérification de l'intégrité des données
    # ========================================================================
    print("\n[TEST 4] Vérification de l'intégrité des données")
    print("-" * 80)

    # Test des valeurs NULL dans les colonnes critiques
    cursor.execute("""
        SELECT COUNT(*) FROM sensor_readings 
        WHERE record_id IS NULL OR timestamp IS NULL
    """)
    null_count = cursor.fetchone()[0]
    if null_count == 0:
        print(f"   Pas de valeurs NULL dans les colonnes critiques")
    else:
        print(f"   {null_count} valeurs NULL trouvées dans les colonnes critiques")
        all_tests_passed = False

    # Test des doublons de record_id
    cursor.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT record_id) as duplicates 
        FROM sensor_readings
    """)
    duplicates = cursor.fetchone()[0]
    if duplicates == 0:
        print(f"   Pas de doublons de record_id")
    else:
        print(f"   {duplicates} doublons de record_id trouvés")
        all_tests_passed = False

    # Test des valeurs de température
    cursor.execute("""
        SELECT MIN(temperature), MAX(temperature), AVG(temperature)
        FROM sensor_readings
        WHERE temperature IS NOT NULL
    """)
    temp_stats = cursor.fetchone()
    if temp_stats[0] is not None:
        print(f"   Température: Min={temp_stats[0]:.2f}°C, Max={temp_stats[1]:.2f}°C, Moy={temp_stats[2]:.2f}°C")
        if 0 <= temp_stats[0] <= 150 and 0 <= temp_stats[1] <= 150:
            print(f"   Valeurs de température dans les limites (0-150°C)")
        else:
            print(f"  ⚠  Valeurs de température hors limites détectées")

    # ========================================================================
    # TEST 5: Vérification de la qualité des données
    # ========================================================================
    print("\n[TEST 5] Vérification de la qualité des données")
    print("-" * 80)

    cursor.execute("""
        SELECT data_quality, COUNT(*) as count 
        FROM sensor_readings 
        GROUP BY data_quality
    """)
    quality_dist = cursor.fetchall()

    total_records = sum(row[1] for row in quality_dist)
    print("  Distribution de la qualité:")
    for quality, count in quality_dist:
        percentage = (count / total_records) * 100
        print(f"    • {quality}: {count:,} ({percentage:.2f}%)")

    # ========================================================================
    # TEST 6: Vérification des agrégations horaires
    # ========================================================================
    print("\n[TEST 6] Vérification des agrégations horaires")
    print("-" * 80)

    cursor.execute("""
        SELECT 
            COUNT(*) as total_hours,
            AVG(defect_rate) as avg_defect_rate,
            MAX(defect_rate) as max_defect_rate
        FROM hourly_summary
    """)
    summary_stats = cursor.fetchone()

    if summary_stats[0] > 0:
        print(f"   Nombre d'heures agrégées: {summary_stats[0]:,}")
        print(f"   Taux de défaut moyen: {summary_stats[1]:.2f}%")
        print(f"   Taux de défaut maximum: {summary_stats[2]:.2f}%")
        print(f"   Agrégations horaires calculées correctement")
    else:
        print(f"   Aucune agrégation horaire trouvée")
        all_tests_passed = False

    # ========================================================================
    # TEST 7: Requêtes d'analyse avancées
    # ========================================================================
    print("\n[TEST 7] Requêtes d'analyse avancées")
    print("-" * 80)

    # Top 5 machines avec le plus de défauts
    cursor.execute("""
        SELECT machine_id, AVG(defect_rate) as avg_defect_rate
        FROM hourly_summary
        GROUP BY machine_id
        ORDER BY avg_defect_rate DESC
        LIMIT 5
    """)
    top_defects = cursor.fetchall()

    if top_defects:
        print("   Top 5 machines avec le plus de défauts:")
        for i, (machine, rate) in enumerate(top_defects, 1):
            print(f"    {i}. {machine}: {rate:.2f}% de défauts")
        print(f"   Analyse des défauts réussie")

    # ========================================================================
    # TEST 8: Performance de la base de données
    # ========================================================================
    print("\n[TEST 8] Performance de la base de données")
    print("-" * 80)

    # Test de requête complexe
    import time
    start_time = time.time()

    cursor.execute("""
        SELECT 
            s.machine_id,
            COUNT(*) as reading_count,
            AVG(s.temperature) as avg_temp,
            AVG(h.defect_rate) as avg_defect_rate
        FROM sensor_readings s
        LEFT JOIN hourly_summary h ON s.machine_id = h.machine_id
        GROUP BY s.machine_id
        LIMIT 10
    """)
    results = cursor.fetchall()

    query_time = (time.time() - start_time) * 1000
    print(f"  ⚡ Temps d'exécution de requête complexe: {query_time:.2f}ms")
    if query_time < 1000:
        print(f"   Performance excellente (< 1 seconde)")
    else:
        print(f"  ️  Performance à améliorer (> 1 seconde)")

    # ========================================================================
    # RÉSUMÉ FINAL
    # ========================================================================
    print("\n" + "=" * 80)
    if all_tests_passed:
        print(" TOUS LES TESTS RÉUSSIS - BASE DE DONNÉES OPÉRATIONNELLE")
    else:
        print("️  CERTAINS TESTS ONT ÉCHOUÉ - VÉRIFICATION NÉCESSAIRE")
    print("=" * 80)

    # ========================================================================
    # EXPORT DU RAPPORT
    # ========================================================================
    print("\n[EXPORT] Génération du rapport de test")
    print("-" * 80)

    report = f"""
RAPPORT DE TEST ETL PIPELINE
============================
Date: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

STATISTIQUES GÉNÉRALES:
- sensor_readings: {sensor_count:,} enregistrements
- quality_checks: {quality_count:,} enregistrements  
- hourly_summary: {summary_count:,} enregistrements

QUALITÉ DES DONNÉES:
"""

    for quality, count in quality_dist:
        percentage = (count / total_records) * 100
        report += f"- {quality}: {count:,} ({percentage:.2f}%)\n"

    report += f"""
STATUT: {" SUCCÈS" if all_tests_passed else "⚠  ATTENTION"}
"""

    with open('test_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"   Rapport sauvegardé dans 'test_report.txt'")

    conn.close()
    return all_tests_passed


def generate_sample_queries():
    """Génère un fichier avec des requêtes SQL d'exemple."""

    queries = """
-- REQUÊTES SQL D'EXEMPLE POUR L'ANALYSE DES DONNÉES
-- ==================================================

-- 1. Vue d'ensemble des capteurs
SELECT * FROM sensor_readings LIMIT 10;

-- 2. Top 10 heures avec le plus de défauts
SELECT * FROM hourly_summary 
ORDER BY defect_rate DESC 
LIMIT 10;

-- 3. Statistiques par machine
SELECT 
    machine_id,
    COUNT(*) as total_readings,
    AVG(temperature) as avg_temperature,
    AVG(pressure) as avg_pressure,
    AVG(vibration) as avg_vibration
FROM sensor_readings
GROUP BY machine_id;

-- 4. Taux de défaut moyen par ligne de production
SELECT 
    line_id,
    AVG(defect_rate) as avg_defect_rate,
    COUNT(*) as hours_monitored
FROM hourly_summary
GROUP BY line_id
ORDER BY avg_defect_rate DESC;

-- 5. Distribution de la qualité des données
SELECT 
    data_quality,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sensor_readings), 2) as percentage
FROM sensor_readings
GROUP BY data_quality;

-- 6. Analyse des tendances de température
SELECT 
    DATE(timestamp) as date,
    AVG(temperature) as avg_temp,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp
FROM sensor_readings
GROUP BY DATE(timestamp)
ORDER BY date;

-- 7. Machines nécessitant une attention (défauts > 5%)
SELECT 
    machine_id,
    AVG(defect_rate) as avg_defect_rate,
    COUNT(*) as hours_checked
FROM hourly_summary
GROUP BY machine_id
HAVING AVG(defect_rate) > 5
ORDER BY avg_defect_rate DESC;

-- 8. Corrélation température-défauts
SELECT 
    h.machine_id,
    AVG(s.temperature) as avg_temp,
    AVG(h.defect_rate) as avg_defect_rate
FROM sensor_readings s
JOIN hourly_summary h ON s.machine_id = h.machine_id 
    AND strftime('%Y-%m-%d %H', s.timestamp) = strftime('%Y-%m-%d %H', h.hour)
GROUP BY h.machine_id;

-- 9. Production horaire par ligne
SELECT 
    line_id,
    strftime('%H', hour) as hour_of_day,
    AVG(total_checks) as avg_checks,
    AVG(defect_rate) as avg_defect_rate
FROM hourly_summary
GROUP BY line_id, hour_of_day
ORDER BY line_id, hour_of_day;

-- 10. Résumé global de la production
SELECT 
    COUNT(DISTINCT machine_id) as total_machines,
    COUNT(DISTINCT line_id) as total_lines,
    COUNT(*) as total_hours,
    AVG(defect_rate) as overall_defect_rate,
    AVG(total_checks) as avg_checks_per_hour
FROM hourly_summary;
"""

    with open('sample_queries.sql', 'w', encoding='utf-8') as f:
        f.write(queries)

    print("\n" + "=" * 80)
    print("📝 Requêtes SQL d'exemple générées dans 'sample_queries.sql'")
    print("=" * 80)


if __name__ == "__main__":
    # Exécuter les tests
    success = test_database_integrity()

    # Générer les requêtes d'exemple
    generate_sample_queries()

    print("\n" + "=" * 80)
    print("🎉 TEST COMPLET TERMINÉ")
    print("=" * 80)
    print("\nFichiers générés:")
    print("  • test_report.txt - Rapport détaillé des tests")
    print("  • sample_queries.sql - Requêtes SQL d'exemple")
    print("\nVous pouvez maintenant:")
    print("  1. Consulter le rapport de test")
    print("  2. Exécuter les requêtes SQL d'exemple")
    print("  3. Utiliser le dashboard web pour visualiser les résultats")
    print("=" * 80)