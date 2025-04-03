import 'jest';
import { greeting } from './index';

describe('greeting', () => {
  it('should return hello message with the provided name', () => {
    expect(greeting('TypeScript')).toBe('Hello, TypeScript!');
  });
}); 