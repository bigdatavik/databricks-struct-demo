# Databricks STRUCT Aggregation Demo

A comprehensive demonstration of modern approaches to aggregate data within STRUCT arrays in Databricks, showcasing both higher-order functions and LATERAL VIEW EXPLODE patterns.

## 🎯 Overview

This repository contains practical examples for aggregating `chargeAmount` fields within nested STRUCT arrays in Databricks SQL. It demonstrates multiple approaches to solve common data aggregation challenges while maintaining data structure integrity.

## 📋 Use Case

Consider a claims processing scenario where you have:
- **Claim Headers**: Contains claim metadata (ID, line of business, total charges)
- **Claim Details**: Array of line items with individual charges and units

**Goal**: Calculate the sum of all `chargeAmount` values in the `claimDetail` array and rebuild the STRUCT with the correct `totalCharges`.

## 🏗️ Data Structure

```sql
{
  "claimHeader": {
    "claimId": "ABC123456789",
    "lineOfBusiness": "Medicaid", 
    "totalCharges": 3.25
  },
  "claimDetail": [
    {"chargeAmount": 1.25, "units": 1.00},
    {"chargeAmount": 2.00, "units": 1.00}
  ]
}
```

## 🚀 Methods Demonstrated

### Method 1: Higher-Order Functions with `aggregate()`
✅ **Recommended for performance**
- Uses `aggregate()` function for in-place array processing
- No row expansion, maintains data structure
- Best for simple aggregations

```sql
SELECT 
    claimHeader.claimId,
    aggregate(
        claimDetail, 
        CAST(0.0 AS DOUBLE),
        (acc, detail) -> acc + detail.chargeAmount
    ) as calculated_totalCharges
FROM claims_table
```

### Method 2: Higher-Order Functions with `transform()` and `reduce()`
- Extracts values first, then reduces
- Two-step approach for complex transformations

```sql
SELECT 
    reduce(
        transform(claimDetail, detail -> detail.chargeAmount),
        CAST(0.0 AS DOUBLE),
        (acc, x) -> acc + x
    ) as calculated_totalCharges
FROM claims_table
```

### Method 3: STRUCT Reconstruction
- Rebuilds the complete data structure with calculated values
- Perfect for ETL pipelines

```sql
SELECT 
    struct(
        claimHeader.claimId as claimId,
        claimHeader.lineOfBusiness as lineOfBusiness,
        aggregate(claimDetail, CAST(0.0 AS DOUBLE), 
                 (acc, detail) -> acc + detail.chargeAmount) as totalCharges
    ) as claimHeader,
    claimDetail
FROM claims_table
```

### Method 4: LATERAL VIEW EXPLODE
✅ **Recommended for complex aggregations**
- Familiar SQL syntax with `SUM()`, `COUNT()`, `AVG()`
- Great for multiple aggregation functions
- Easy to understand for SQL developers

```sql
SELECT 
    claimHeader.claimId,
    claimHeader.lineOfBusiness,
    SUM(detail.chargeAmount) as total_charges,
    COUNT(*) as line_item_count,
    AVG(detail.chargeAmount) as avg_charge_per_line
FROM claims_table
LATERAL VIEW EXPLODE(claimDetail) t AS detail
GROUP BY claimHeader.claimId, claimHeader.lineOfBusiness
```

## 🔧 Prerequisites

- **Databricks Runtime**: 12.3 LTS or higher
- **Spark Version**: 3.0+
- **SQL Analytics**: Supported in Databricks SQL warehouses

## 📖 Getting Started

1. **Clone this repository**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/databricks-struct-demo.git
   cd databricks-struct-demo
   ```

2. **Import the notebook** into your Databricks workspace:
   - Upload `struct_aggregation_demo.ipynb`
   - Or use Databricks CLI/Git integration

3. **Run the cells sequentially** to see each method in action

## 🎛️ Running the Demo

### Step 1: Data Setup
The notebook creates sample data that matches typical claims processing structures:

```python
sample_data = [
    {
        "claimHeader": {
            "claimId": "ABC123456789",
            "lineOfBusiness": "Medicaid",
            "totalCharges": 3.25
        },
        "claimDetail": [
            {"chargeAmount": 1.25, "units": 1.00},
            {"chargeAmount": 2.00, "units": 1.00}
        ]
    }
    # ... more sample records
]
```

### Step 2: Try Different Methods
Each cell demonstrates a different aggregation approach with detailed explanations and expected outputs.

### Step 3: Performance Comparison
Compare execution times and resource usage between methods for your specific use case.

## 📊 Method Comparison

| Method | Performance | Complexity | Use Case |
|--------|-------------|------------|----------|
| `aggregate()` | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Simple aggregations, large arrays |
| `transform() + reduce()` | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Complex transformations |
| STRUCT Reconstruction | ⭐⭐⭐⭐ | ⭐⭐⭐ | ETL pipelines, data modeling |
| LATERAL VIEW EXPLODE | ⭐⭐⭐ | ⭐⭐ | Complex aggregations, SQL familiarity |

## 🏆 Best Practices

### When to Use Higher-Order Functions:
- ✅ Large nested arrays (better memory efficiency)
- ✅ Simple aggregations
- ✅ Performance-critical applications
- ✅ Maintaining data structure without reconstruction

### When to Use LATERAL VIEW EXPLODE:
- ✅ Complex multi-column aggregations
- ✅ Multiple aggregation functions needed
- ✅ SQL developer familiarity is important
- ✅ Ad-hoc analysis and exploration

### General Tips:
- Always use explicit type casting: `CAST(0.0 AS DOUBLE)`
- Test with your actual data volumes
- Consider memory constraints for large arrays
- Use appropriate cluster sizes for your workload

## 🔍 Troubleshooting

### Common Issues:

**Data Type Mismatch Error**:
```
DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE
```
**Solution**: Use explicit casting: `CAST(0.0 AS DOUBLE)`

**Column Resolution Error**:
```
UNRESOLVED_COLUMN.WITHOUT_SUGGESTION
```
**Solution**: Check LATERAL VIEW syntax and column scoping

**Performance Issues**:
- For large arrays: Prefer higher-order functions
- For complex aggregations: Use LATERAL VIEW EXPLODE
- Consider partitioning and cluster sizing

## 📚 Additional Resources

- [Databricks Higher-Order Functions Documentation](https://docs.databricks.com/sql/language-manual/functions/aggregate.html)
- [Spark SQL Guide - Higher-Order Functions](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#higher-order-functions)
- [Databricks SQL Analytics Best Practices](https://docs.databricks.com/sql/admin/sql-endpoints.html)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Vik Malhotra**
- 📧 Email: vik.malhotra@databricks.com
- 💼 LinkedIn: [Vik Malhotra](https://www.linkedin.com/in/vkmalhotra/)
- 🐙 GitHub: [@bigdatavik](https://github.com/bigdatavik)

## 🙏 Acknowledgments

- Databricks community for SQL best practices
- Apache Spark contributors for higher-order functions
- Claims processing teams for real-world use case insights

---

⭐ **Star this repository if it helped you!** ⭐
