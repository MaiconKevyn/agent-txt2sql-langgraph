"""
SQL Validation Service - Comprehensive validation for generated SQL queries
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import re
import logging


class ValidationSeverity(Enum):
    """Validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationIssue:
    """Individual validation issue"""
    code: str
    severity: ValidationSeverity
    message: str
    suggested_fix: Optional[str] = None
    line_number: Optional[int] = None


@dataclass
class ValidationResult:
    """Complete validation result"""
    is_valid: bool
    is_safe: bool
    score: float  # 0-100 quality score
    issues: List[ValidationIssue]
    corrected_sql: Optional[str] = None
    
    @property
    def has_critical_issues(self) -> bool:
        return any(issue.severity == ValidationSeverity.CRITICAL for issue in self.issues)
    
    @property
    def has_errors(self) -> bool:
        return any(issue.severity == ValidationSeverity.ERROR for issue in self.issues)


class ISQLValidationService(ABC):
    """Interface for SQL validation service"""
    
    @abstractmethod
    def validate_sql(self, sql: str, query_intent: str = "") -> ValidationResult:
        """Validate SQL query comprehensively"""
        pass
    
    @abstractmethod
    def suggest_corrections(self, sql: str, issues: List[ValidationIssue]) -> Optional[str]:
        """Suggest corrected SQL based on identified issues"""
        pass


class ComprehensiveSQLValidationService(ISQLValidationService):
    """Comprehensive SQL validation implementation"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Prevent duplicate logs by disabling propagation to root logger
        self.logger.propagate = False
        self._setup_validation_rules()
    
    def _setup_validation_rules(self):
        """Setup validation rules and patterns"""
        # Security patterns
        self.dangerous_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE",
            "EXEC", "EXECUTE", "xp_", "sp_", "BULK", "OPENROWSET"
        ]
        
        # Common GROUP BY issues patterns
        self.group_by_patterns = [
            r'GROUP\s+BY\s+(\w+)',
            r'GROUP\s+BY\s+([^,\s]+(?:\s*,\s*[^,\s]+)*)'
        ]
        
        # Valid table names
        self.valid_tables = {"sus_data", "cid_capitulos"}
        
        # Valid column patterns
        self.valid_columns = {
            "sus_data": {
                "DIAG_PRINC", "MUNIC_RES", "MUNIC_MOV", "PROC_REA", "IDADE", 
                "SEXO", "CID_MORTE", "MORTE", "CNES", "VAL_TOT", "UTI_MES_TO",
                "DT_INTER", "DT_SAIDA", "total_ocorrencias", "UF_RESIDENCIA_PACIENTE",
                "CIDADE_RESIDENCIA_PACIENTE", "LATI_CIDADE_RES", "LONG_CIDADE_RES"
            },
            "cid_capitulos": {"categoria_geral", "codigo_inicio", "codigo_fim"}
        }
    
    def validate_sql(self, sql: str, query_intent: str = "") -> ValidationResult:
        """Comprehensive SQL validation"""
        issues = []
        score = 100.0
        
        # Security validation
        security_issues = self._validate_security(sql)
        issues.extend(security_issues)
        score -= len([i for i in security_issues if i.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]]) * 30
        
        # Syntax validation
        syntax_issues = self._validate_syntax(sql)
        issues.extend(syntax_issues)
        score -= len([i for i in syntax_issues if i.severity == ValidationSeverity.ERROR]) * 20
        
        # GROUP BY validation (CRITICAL for our use case)
        groupby_issues = self._validate_group_by_logic(sql)
        issues.extend(groupby_issues)
        score -= len([i for i in groupby_issues if i.severity == ValidationSeverity.CRITICAL]) * 50
        
        # Date handling validation
        date_issues = self._validate_date_handling(sql)
        issues.extend(date_issues)
        score -= len([i for i in date_issues if i.severity == ValidationSeverity.ERROR]) * 25
        
        # Schema validation
        schema_issues = self._validate_schema_usage(sql)
        issues.extend(schema_issues)
        score -= len([i for i in schema_issues if i.severity == ValidationSeverity.WARNING]) * 5
        
        # Best practices validation
        practice_issues = self._validate_best_practices(sql, query_intent)
        issues.extend(practice_issues)
        score -= len([i for i in practice_issues if i.severity == ValidationSeverity.WARNING]) * 3
        
        # Calculate final scores
        score = max(0.0, min(100.0, score))
        is_valid = score >= 70.0 and not any(i.severity in [ValidationSeverity.CRITICAL, ValidationSeverity.ERROR] for i in issues)
        is_safe = not any(i.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL] for i in issues)
        
        # Generate corrected SQL if needed
        corrected_sql = None
        if not is_valid and issues:
            corrected_sql = self.suggest_corrections(sql, issues)
        
        return ValidationResult(
            is_valid=is_valid,
            is_safe=is_safe,
            score=score,
            issues=issues,
            corrected_sql=corrected_sql
        )
    
    def _validate_security(self, sql: str) -> List[ValidationIssue]:
        """Validate SQL security"""
        issues = []
        sql_upper = sql.upper()
        
        for keyword in self.dangerous_keywords:
            if keyword in sql_upper:
                issues.append(ValidationIssue(
                    code="SEC001",
                    severity=ValidationSeverity.CRITICAL,
                    message=f"Dangerous keyword detected: {keyword}",
                    suggested_fix="Remove dangerous operations"
                ))
        
        # Check for SQL injection patterns
        injection_patterns = [
            r"--",  # SQL comments
            r"/\*.*\*/",  # Block comments
            r";.*DROP",  # Multiple statements
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(ValidationIssue(
                    code="SEC002",
                    severity=ValidationSeverity.ERROR,
                    message=f"Potential SQL injection pattern: {pattern}",
                    suggested_fix="Remove suspicious patterns"
                ))
        
        return issues
    
    def _validate_syntax(self, sql: str) -> List[ValidationIssue]:
        """Validate SQL syntax"""
        issues = []
        
        # Basic syntax checks
        if not sql.strip():
            issues.append(ValidationIssue(
                code="SYN001",
                severity=ValidationSeverity.CRITICAL,
                message="Empty SQL query",
                suggested_fix="Provide a valid SQL query"
            ))
            return issues
        
        # Check for SELECT statement
        if not sql.upper().strip().startswith("SELECT"):
            issues.append(ValidationIssue(
                code="SYN002",
                severity=ValidationSeverity.ERROR,
                message="Query must start with SELECT",
                suggested_fix="Start query with SELECT statement"
            ))
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            issues.append(ValidationIssue(
                code="SYN003",
                severity=ValidationSeverity.ERROR,
                message="Unbalanced parentheses in query",
                suggested_fix="Balance parentheses in expressions"
            ))
        
        return issues
    
    def _validate_group_by_logic(self, sql: str) -> List[ValidationIssue]:
        """🚨 CRITICAL: Validate GROUP BY logic - this is our main problem!"""
        issues = []
        sql_upper = sql.upper()
        
        # Check if query has GROUP BY
        if "GROUP BY" not in sql_upper:
            return issues  # No GROUP BY, no issues
        
        # Extract GROUP BY columns
        group_by_match = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+ORDER|\s+HAVING|\s+LIMIT|\s*;|$)', sql_upper)
        if not group_by_match:
            return issues
        
        group_by_clause = group_by_match.group(1).strip()
        
        # Extract SELECT columns (before FROM)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
        if not select_match:
            issues.append(ValidationIssue(
                code="GRP001",
                severity=ValidationSeverity.ERROR,
                message="Cannot parse SELECT clause",
                suggested_fix="Ensure proper SELECT ... FROM structure"
            ))
            return issues
        
        select_clause = select_match.group(1).strip()
        
        # 🚨 CRITICAL CHECK: If GROUP BY contains a column, it MUST be in SELECT
        group_by_columns = [col.strip() for col in group_by_clause.split(',')]
        
        for group_col in group_by_columns:
            # Clean up column name (remove table alias if present)
            clean_col = group_col.split('.')[-1] if '.' in group_col else group_col
            
            # Check if this column appears in SELECT clause
            # Need to be more careful about checking - look for the actual column name
            col_in_select = (clean_col in select_clause or 
                           group_col in select_clause or
                           f"{clean_col}," in select_clause or
                           f" {clean_col} " in select_clause)
            
            if not col_in_select:
                # Special check for CIDADE_RESIDENCIA_PACIENTE (our main issue)
                if "CIDADE_RESIDENCIA_PACIENTE" in group_col:
                    issues.append(ValidationIssue(
                        code="GRP002",
                        severity=ValidationSeverity.CRITICAL,
                        message=f"GROUP BY column '{group_col}' not in SELECT clause - results will be meaningless",
                        suggested_fix=f"Add '{group_col}' to SELECT clause or remove from GROUP BY"
                    ))
                else:
                    issues.append(ValidationIssue(
                        code="GRP003",
                        severity=ValidationSeverity.ERROR,
                        message=f"GROUP BY column '{group_col}' not in SELECT clause",
                        suggested_fix=f"Add '{group_col}' to SELECT clause"
                    ))
        
        return issues
    
    def _validate_date_handling(self, sql: str) -> List[ValidationIssue]:
        """Validate date handling (JULIANDAY requirement)"""
        issues = []
        sql_upper = sql.upper()
        
        # Check for arithmetic date subtraction (our common issue)
        if 'AVG' in sql_upper and 'DT_SAIDA' in sql_upper and 'DT_INTER' in sql_upper:
            if 'JULIANDAY' not in sql_upper and 'DT_SAIDA - DT_INTER' in sql_upper.replace(' ', ''):
                issues.append(ValidationIssue(
                    code="DATE001",
                    severity=ValidationSeverity.CRITICAL,
                    message="Arithmetic date subtraction detected - use JULIANDAY for correct calculations",
                    suggested_fix="Use JULIANDAY(SUBSTR(...)) conversion for date arithmetic"
                ))
        
        # Check for proper date format usage
        date_formats = [r"'\d{4}-\d{2}-\d{2}'", r"DATE\(", r"STRFTIME\("]
        for pattern in date_formats:
            if re.search(pattern, sql):
                issues.append(ValidationIssue(
                    code="DATE002",
                    severity=ValidationSeverity.WARNING,
                    message="Date format detected - ensure compatibility with INTEGER dates (YYYYMMDD)",
                    suggested_fix="Use INTEGER format YYYYMMDD for date comparisons"
                ))
        
        return issues
    
    def _validate_schema_usage(self, sql: str) -> List[ValidationIssue]:
        """Validate schema usage (table and column names)"""
        issues = []
        sql_upper = sql.upper()
        
        # Check for valid table names
        for table in re.findall(r'FROM\s+(\w+)', sql_upper):
            if table.lower() not in self.valid_tables:
                issues.append(ValidationIssue(
                    code="SCH001",
                    severity=ValidationSeverity.WARNING,
                    message=f"Unknown table name: {table}",
                    suggested_fix=f"Use valid table names: {', '.join(self.valid_tables)}"
                ))
        
        return issues
    
    def _validate_best_practices(self, sql: str, query_intent: str) -> List[ValidationIssue]:
        """Validate SQL best practices"""
        issues = []
        sql_upper = sql.upper()
        
        # Check for COUNT with LIMIT (unnecessary only without GROUP BY)
        if 'COUNT(' in sql_upper and 'LIMIT' in sql_upper:
            # LIMIT is only unnecessary if there's no GROUP BY
            if 'GROUP BY' not in sql_upper:
                issues.append(ValidationIssue(
                    code="BP001",
                    severity=ValidationSeverity.WARNING,
                    message="LIMIT with COUNT(*) is unnecessary - COUNT always returns one row",
                    suggested_fix="Remove LIMIT clause from COUNT queries"
                ))
        
        # Check for proper aggregation patterns
        if query_intent and "top" in query_intent.lower() and "cities" in query_intent.lower():
            if "GROUP BY" in sql_upper and "CIDADE_RESIDENCIA_PACIENTE" not in sql_upper:
                issues.append(ValidationIssue(
                    code="BP002",
                    severity=ValidationSeverity.ERROR,
                    message="Query intent suggests city ranking but no city grouping found",
                    suggested_fix="Add GROUP BY CIDADE_RESIDENCIA_PACIENTE for city-based analysis"
                ))
        
        # 🆕 Check for municipality name vs code mismatch
        if query_intent:
            intent_lower = query_intent.lower()
            # User asks for municipality/city names
            if any(word in intent_lower for word in ["municipio", "cidade", "city", "municipality"]):
                # But query selects municipality codes
                if "MUNIC_RES" in sql_upper and "CIDADE_RESIDENCIA_PACIENTE" not in sql_upper:
                    issues.append(ValidationIssue(
                        code="BP003",
                        severity=ValidationSeverity.ERROR,
                        message="User asked for municipality names but query returns municipality codes",
                        suggested_fix="Use CIDADE_RESIDENCIA_PACIENTE instead of MUNIC_RES for city names"
                    ))
        
        return issues
    
    def suggest_corrections(self, sql: str, issues: List[ValidationIssue]) -> Optional[str]:
        """Suggest corrected SQL based on identified issues"""
        corrected_sql = sql
        
        for issue in issues:
            if issue.code == "GRP002":  # CIDADE_RESIDENCIA_PACIENTE missing from SELECT
                # Fix the critical GROUP BY issue
                if "GROUP BY CIDADE_RESIDENCIA_PACIENTE" in corrected_sql.upper():
                    # Add CIDADE_RESIDENCIA_PACIENTE to SELECT
                    select_match = re.search(r'(SELECT\s+)(.*?)(\s+FROM)', corrected_sql, re.IGNORECASE | re.DOTALL)
                    if select_match:
                        select_part = select_match.group(2).strip()
                        if "CIDADE_RESIDENCIA_PACIENTE" not in select_part.upper():
                            new_select = f"{select_match.group(1)}CIDADE_RESIDENCIA_PACIENTE, {select_part}{select_match.group(3)}"
                            corrected_sql = corrected_sql.replace(select_match.group(0), new_select)
            
            elif issue.code == "DATE001":  # Fix arithmetic date subtraction
                # Replace arithmetic subtraction with JULIANDAY
                corrected_sql = re.sub(
                    r'AVG\s*\(\s*DT_SAIDA\s*-\s*DT_INTER\s*\)',
                    '''AVG(
    JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
    JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2))
)''',
                    corrected_sql,
                    flags=re.IGNORECASE
                )
            
            elif issue.code == "BP001":  # Remove unnecessary LIMIT from COUNT
                corrected_sql = re.sub(r'\s+LIMIT\s+\d+', '', corrected_sql, flags=re.IGNORECASE)
            
            elif issue.code == "BP003":  # Fix municipality code vs name mismatch
                # Replace MUNIC_RES with CIDADE_RESIDENCIA_PACIENTE
                corrected_sql = corrected_sql.replace("MUNIC_RES", "CIDADE_RESIDENCIA_PACIENTE")
        
        return corrected_sql if corrected_sql != sql else None


class SQLValidationFactory:
    """Factory for creating SQL validation services"""
    
    @staticmethod
    def create_comprehensive_validator() -> ISQLValidationService:
        """Create comprehensive SQL validator"""
        return ComprehensiveSQLValidationService()