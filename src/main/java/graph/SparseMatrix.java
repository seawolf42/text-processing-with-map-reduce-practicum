package graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

// represents a sparse matrix using adjacency lists

public class SparseMatrix<K extends Comparable<? super K>, W> {

	Class<W> classOfW;
	Map<K, Map<K, W>> matrix;
	
	public SparseMatrix(Class<W> classOfW) {
		this.classOfW = classOfW;
		matrix = new HashMap<K, Map<K, W>>();
	}
	
	public void put(K rowKey, K columnKey, W weight) {
		Map<K, W> row;
		if (!matrix.containsKey(rowKey)) {
			row = new HashMap<K, W>();
			matrix.put(rowKey, row);
		} else {
			row = matrix.get(rowKey);
		}

		row.put(columnKey, weight);
	}
	
	public W get(K rowKey, K columnKey) {
		try {
			return matrix.get(rowKey).get(columnKey);
		}
		catch (Exception e) {
			try {
				return classOfW.newInstance();
			} catch (Exception e1) {
				return null;
			}
		}
	}

	public Set<K> getRowKeys() {
		return matrix.keySet();
	}

	public SortedSet<K> getRowKeysSorted() {
		return new TreeSet<K>(getRowKeys());
	}

	public Set<K> getColumnKeys(K rowKey) {
		try {
			return matrix.get(rowKey).keySet();
		}
		catch (Exception e) {
			return new TreeSet<K>();
		}
	}

	public SortedSet<K> getColumnKeysSorted(K rowKey) {
		return new TreeSet<K>(getColumnKeys(rowKey));
	}

	public void clear() {
		matrix.clear();
	}

	public boolean containsKey(K rowKey, K columnKey) {
		try {
			return matrix.get(rowKey).containsKey(columnKey);
		}
		catch (Exception e) {
			return false;
		}
	}

	public boolean isEmpty() {
		for (K key : matrix.keySet()) {
			if (matrix.get(key).size() > 0) {
				return false;
			}
		}
		return true;
	}

	public void remove(K rowKey, K columnKey) {
	}

	public int size() {
		int size = 0;
		for (K key : matrix.keySet()) {
			size += matrix.get(key).size();
		}
		return size;
	}

	public SparseMatrix<K, W> getTranspose() {
		SparseMatrix<K, W> transpose = new SparseMatrix<K, W>(classOfW);
		for (K rowKey : matrix.keySet()) {
			Map<K, W> row = matrix.get(rowKey);
			for (K columnKey : row.keySet()) {
				transpose.put(columnKey, rowKey, row.get(columnKey));
			}
		}
		return transpose;
	}
}
