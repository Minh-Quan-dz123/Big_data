// Tự động tạo mảng 100 phần tử từ U000001 -> U000100
export const USERS = Array.from({ length: 100 }, (_, i) => {
  // padStart(6, '0') giúp chèn thêm số 0 đằng trước cho đủ 6 ký tự
  return `U${String(i + 1).padStart(6, '0')}`;
});