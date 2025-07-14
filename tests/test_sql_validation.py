"""
Test cases for SQL validation service
"""
import pytest
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from application.services.sql_validation_service import (
    ComprehensiveSQLValidationService, 
    ValidationSeverity,
    SQLValidationFactory
)


class TestSQLValidationService:
    
    def setup_method(self):
        """Setup test fixtures"""
        self.validator = SQLValidationFactory.create_comprehensive_validator()
    
    def test_group_by_validation_critical_issue(self):
        """Test the critical GROUP BY issue that was causing problems"""
        # This is the problematic SQL from our original issue
        problematic_sql = """
        SELECT COUNT(*) AS total_mortes, 
               SUM(CASE WHEN SEXO = 3 AND MORTE = 1 THEN 1 ELSE 0 END) AS mortes_mulheres 
        FROM sus_data 
        WHERE MORTE = 1 AND SEXO = 3 
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC 
        LIMIT 5;
        """
        
        result = self.validator.validate_sql(problematic_sql, "top 5 cities with most female deaths")
        
        # Should detect the critical GROUP BY issue
        assert result.has_critical_issues
        assert not result.is_valid
        assert result.score < 70.0
        
        # Should find the specific issue
        group_by_issues = [i for i in result.issues if i.code == "GRP002"]
        assert len(group_by_issues) == 1
        assert "CIDADE_RESIDENCIA_PACIENTE" in group_by_issues[0].message
        
        # Should provide corrected SQL
        assert result.corrected_sql is not None
        assert "CIDADE_RESIDENCIA_PACIENTE," in result.corrected_sql
    
    def test_corrected_sql_validation(self):
        """Test that corrected SQL passes validation"""
        correct_sql = """
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) AS mortes_mulheres 
        FROM sus_data 
        WHERE SEXO = 3 AND MORTE = 1 
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY mortes_mulheres DESC 
        LIMIT 5;
        """
        
        result = self.validator.validate_sql(correct_sql, "top 5 cities with most female deaths")
        
        # Should pass validation
        assert not result.has_critical_issues
        assert result.is_valid
        assert result.score >= 90.0
    
    def test_date_arithmetic_validation(self):
        """Test validation of date arithmetic (JULIANDAY requirement)"""
        # Problematic arithmetic subtraction
        problematic_sql = """
        SELECT AVG(DT_SAIDA - DT_INTER) AS tempo_medio 
        FROM sus_data 
        WHERE DIAG_PRINC LIKE 'J%';
        """
        
        result = self.validator.validate_sql(problematic_sql)
        
        # Should detect critical date issue
        assert result.has_critical_issues
        date_issues = [i for i in result.issues if i.code == "DATE001"]
        assert len(date_issues) == 1
        assert "JULIANDAY" in date_issues[0].suggested_fix
        
        # Should provide corrected SQL with JULIANDAY
        assert result.corrected_sql is not None
        assert "JULIANDAY" in result.corrected_sql
    
    def test_security_validation(self):
        """Test security validation"""
        dangerous_sql = "DROP TABLE sus_data; SELECT * FROM users;"
        
        result = self.validator.validate_sql(dangerous_sql)
        
        # Should detect security issues
        assert result.has_critical_issues
        assert not result.is_safe
        security_issues = [i for i in result.issues if i.code == "SEC001"]
        assert len(security_issues) == 1
    
    def test_count_with_limit_validation(self):
        """Test unnecessary LIMIT with COUNT validation"""
        sql_with_limit = "SELECT COUNT(*) FROM sus_data WHERE SEXO = 3 LIMIT 10;"
        
        result = self.validator.validate_sql(sql_with_limit)
        
        # Should detect best practice issue
        bp_issues = [i for i in result.issues if i.code == "BP001"]
        assert len(bp_issues) == 1
        assert "LIMIT" in bp_issues[0].message
        
        # Should provide corrected SQL without LIMIT
        assert result.corrected_sql is not None
        assert "LIMIT" not in result.corrected_sql
    
    def test_complex_query_validation(self):
        """Test validation of complex queries with multiple issues"""
        complex_sql = """
        SELECT COUNT(*) AS total, AVG(DT_SAIDA - DT_INTER)
        FROM sus_data s
        GROUP BY CIDADE_RESIDENCIA_PACIENTE
        LIMIT 5;
        """
        
        result = self.validator.validate_sql(complex_sql)
        
        # Should detect multiple issues
        assert len(result.issues) >= 2
        
        # Should detect GROUP BY issue (missing city in SELECT)
        group_issues = [i for i in result.issues if i.code.startswith("GRP")]
        assert len(group_issues) >= 1
        
        # Should detect date arithmetic issue
        date_issues = [i for i in result.issues if i.code == "DATE001"]
        assert len(date_issues) == 1
        
        # Should detect COUNT with LIMIT issue
        bp_issues = [i for i in result.issues if i.code == "BP001"]
        assert len(bp_issues) == 1


class TestSQLValidationIntegration:
    """Integration tests for SQL validation in real scenarios"""
    
    def setup_method(self):
        self.validator = ComprehensiveSQLValidationService()
    
    def test_original_problem_case(self):
        """Test the exact SQL that was causing our original problem"""
        # This is the SQL generated by the system that was problematic
        original_problematic_sql = """
        SELECT COUNT(*) AS total_mortes, 
               SUM(CASE WHEN SEXO = 3 AND MORTE = 1 THEN 1 ELSE 0 END) AS mortes_mulheres 
        FROM sus_data 
        WHERE MORTE = 1 AND SEXO = 3 
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC 
        LIMIT 5;
        """
        
        result = self.validator.validate_sql(
            original_problematic_sql, 
            "Quais foram as 5 cidades com mais mortes de mulheres?"
        )
        
        # Validate detection of issues
        assert result.has_critical_issues
        assert "CIDADE_RESIDENCIA_PACIENTE" in str(result.issues)
        
        # Validate correction
        corrected_sql = result.corrected_sql
        assert corrected_sql is not None
        assert "CIDADE_RESIDENCIA_PACIENTE," in corrected_sql  # Should be in SELECT
        
        # Validate corrected SQL
        corrected_result = self.validator.validate_sql(corrected_sql)
        assert corrected_result.is_valid
        assert corrected_result.score >= 85.0
    
    def test_common_query_patterns(self):
        """Test validation of common query patterns"""
        test_cases = [
            # Valid simple query
            ("SELECT COUNT(*) FROM sus_data WHERE SEXO = 3;", True, 95.0),
            
            # Invalid GROUP BY query
            ("SELECT COUNT(*) FROM sus_data GROUP BY CIDADE_RESIDENCIA_PACIENTE;", False, 50.0),
            
            # Valid GROUP BY query
            ("SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) FROM sus_data GROUP BY CIDADE_RESIDENCIA_PACIENTE;", True, 95.0),
            
            # Date arithmetic issue
            ("SELECT AVG(DT_SAIDA - DT_INTER) FROM sus_data;", False, 50.0),
            
            # Security issue
            ("DROP TABLE sus_data;", False, 0.0),
        ]
        
        for sql, should_be_valid, min_score in test_cases:
            result = self.validator.validate_sql(sql)
            
            if should_be_valid:
                assert result.is_valid, f"Query should be valid: {sql}"
                assert result.score >= min_score, f"Score too low for: {sql}"
            else:
                assert not result.is_valid, f"Query should be invalid: {sql}"
                assert result.score <= min_score, f"Score too high for invalid query: {sql}"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])