package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

    private Connection conn;

    // CONSTRUCTOR
    public SellerDaoJDBC(Connection conn) {
        this.conn = conn;
    }

    // Insert seller
    @Override
    public void insert(Seller obj) {
       PreparedStatement st = null;

       try {
            st = conn.prepareStatement(
                "INSERT INTO seller "
                + "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
                + "VALUES "
                + "(?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS
            );

            if (obj.getName() == null)
                throw new DbException("Name can't be null");
            if (obj.getEmail() == null)
                throw new DbException("Email can't be null");
            if (obj.getDepartment() == null)
                throw new DbException("Department can't be null");
            if (obj.getBirthDate() == null)
                throw new DbException("Birth Date can't be null");
            if (obj.getBaseSalary() == null)
                throw new DbException("Base salary can't be null");

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());

            int rowsAffected = st.executeUpdate();
            
            if (rowsAffected > 0) {
                ResultSet rs = st.getGeneratedKeys();
                if (rs.next()) {
                    int id = rs.getInt(1);
                    obj.setId(id);
                }
                DB.closeResultSet(rs);
            } else {
                throw new DbException("Unexpected error! No rows affected!");
            }
       } catch (SQLException e) {
           throw new DbException(e.getMessage());
       } finally {
           DB.closeStatement(st);
       }
    }

    // Updated seller
    @Override
    public void updated(Seller obj) {
        PreparedStatement st = null;

       try {
            st = conn.prepareStatement(
                "UPDATE seller "
                + "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
                + "WHERE Id = ?"
            );

            if (obj.getId() == null)
                throw new DbException("Id can't be null");
            if (obj.getName() == null)
                throw new DbException("Name can't be null");
            if (obj.getEmail() == null)
                throw new DbException("Email can't be null");
            if (obj.getDepartment() == null)
                throw new DbException("Department can't be null");
            if (obj.getBirthDate() == null)
                throw new DbException("Birth Date can't be null");
            if (obj.getBaseSalary() == null)
                throw new DbException("Base salary can't be null");

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());
            st.setInt(6, obj.getId());

            st.executeUpdate();
       } catch (SQLException e) {
           throw new DbException(e.getMessage());
       } finally {
           DB.closeStatement(st);
       }    
    }

    // Delete seller
    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;

        try {
            st = conn.prepareStatement(
                "DELETE FROM seller "
                + "WHERE Id = ?",
                Statement.RETURN_GENERATED_KEYS
            );

            st.setInt(1, id);

            int rowsAffected = st.executeUpdate();

            if (rowsAffected == 0)
                throw new DbException("This id doesn't exist in the database");

        } catch (SQLException e) {
            throw new DbIntegrityException(e.getMessage());
        } finally {
            DB.closeStatement(st);
        }
    }

    // Find By Id
    @Override
    public Seller findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                "select seller.*, department.Name as DepName "
                + "from seller inner join department on seller.DepartmentId = departmentId "
                + "where seller.id = ?"
            );
            
            st.setInt(1, id);
            rs = st.executeQuery();

            if (rs.next()) {
                Department dep = instanciateDepartment(rs);
                Seller obj = instaciateSeller(rs, dep);
                return obj;
            }
            return null;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    // Instaciate Seller
    private Seller instaciateSeller(ResultSet rs, Department dep) throws SQLException {
        Seller obj = new Seller();
        obj.setId(rs.getInt("Id"));
        obj.setName(rs.getString("Name"));
        obj.setEmail(rs.getString("Email"));
        obj.setBaseSalary(rs.getDouble("BaseSalary"));
        obj.setBirthDate(new java.util.Date(rs.getTimestamp("BirthDate").getTime()));
        obj.setDepartment(dep);
        return obj;
    }

    // Instaciate Seller
    private Department instanciateDepartment(ResultSet rs) throws SQLException {
        Department dep = new Department();
        dep.setId(rs.getInt("DepartmentId"));
        dep.setName(rs.getString("DepName"));
        return dep;
    }

    // Find All
    @Override
    public List<Seller> findAll() {
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                "SELECT seller.*, department.Name as DepName "
                + "FROM seller INNER JOIN department "
                + "ON seller.DepartmentId = department.Id "
                + "ORDER BY Name"
            );

            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap<>();

            while (rs.next()) {
                Department dep = map.get(rs.getInt("DepartmentId"));

                if (dep == null) {
                    dep = instanciateDepartment(rs);
                    map.put(rs.getInt("DepartmentId"), dep);
                }

                Seller obj = instaciateSeller(rs, dep);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
            DB.closeResultSet(rs);
        }
    }

    // Find By Department
    @Override
    public List<Seller> findByDepartment(Department department) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                "select seller.*, department.Name as DepName "
                + "from seller inner join department "
                + "on seller.DepartmentId = department.Id "
                + "where DepartmentId = ? "
                + "order by name"
            );
            
            st.setInt(1, department.getId());

            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap<>();

            while (rs.next()) {

                Department dep = map.get(rs.getInt("DepartmentId"));

                if (dep == null) {
                    dep = instanciateDepartment(rs);
                    map.put(rs.getInt("DepartmentId"), dep);
                }

                Seller obj = instaciateSeller(rs, dep);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }
    
}
